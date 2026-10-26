// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2026 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/hashicorp/raft"
)

// Keep the active check inside Kubernetes' default one-second probe timeout.
const readinessTimeout = 750 * time.Millisecond

type registryVerification struct {
	done chan struct{}
	err  error // published by closing done
}

// CheckReady checks local voter membership, Raft commit catch-up, registry quorum
// contact, and the RPC path used to admit clients.
// It does not claim a client ID or require all configured replicas to exist, so an
// OrderedReady StatefulSet can bootstrap its first voter before starting the rest.
// The caller's deadline is respected, with an upper bound of readinessTimeout.
func (c *Cluster) CheckReady(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, readinessTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.stopping.Load() {
		return net.ErrClosed
	}
	if c.draining.Load() {
		return ErrDraining
	}
	if !c.inContact() {
		return ErrNoQuorum
	}

	// A joining node may hear heartbeats before it has received its registry
	// configuration and committed log. Require local voter membership and catch
	// up to the commit index observed by this probe, rather than contact alone.
	config := c.raft.GetConfiguration()
	if err := config.Error(); err != nil {
		return fmt.Errorf("registry readiness configuration: %w", err)
	}
	voter := false
	for _, server := range config.Configuration().Servers {
		if server.ID == raft.ServerID(c.opts.NodeName) && server.Suffrage == raft.Voter {
			voter = true
			break
		}
	}
	commitIndex := c.raft.CommitIndex()
	if !voter || commitIndex == 0 {
		return fmt.Errorf("registry readiness: local voter has not received a committed log")
	}

	var err error
	if c.raft.State() == raft.Leader {
		err = c.verifyRegistry(ctx)
	} else {
		_, leader := c.raft.LeaderWithID()
		addr := c.peerAddr(string(leader))
		if addr == "" {
			return fmt.Errorf("registry readiness leader %s: %w", leader, errPeerUnknown)
		}
		// An impossible ownership confirmation exercises the same listener and
		// response path as admission. New leaders verify quorum without a log
		// write; older leaders confirm through Raft during a rolling upgrade.
		body := appendString(nil, "")
		body = appendString(body, c.opts.NodeName)
		body = binary.AppendUvarint(body, 0)
		var response []byte
		response, err = c.call(ctx, addr, frameConfirmRequest, body)
		if err == nil && (len(response) != 1 || response[0] != 0) {
			err = ErrMalformedFrame
		}
	}
	if err != nil {
		return fmt.Errorf("registry readiness: %w", err)
	}
	if c.raft.AppliedIndex() < commitIndex {
		// Compare against a fixed index so sustained traffic cannot keep moving
		// the target. Give a healthy node time to catch up within the same budget.
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for c.raft.AppliedIndex() < commitIndex {
			select {
			case <-ctx.Done():
				return fmt.Errorf("registry readiness awaiting committed log: %w", ctx.Err())
			case <-c.done:
				return net.ErrClosed
			case <-ticker.C:
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.stopping.Load() {
		return net.ErrClosed
	}
	if c.draining.Load() {
		return ErrDraining
	}
	if !c.inContact() {
		return ErrNoQuorum
	}
	return nil
}

// verifyRegistry verifies current leadership with the voting quorum. Coalescing
// prevents concurrent HTTP and peer probes from accumulating Raft futures while
// callers independently cancel or reach their deadlines.
func (c *Cluster) verifyRegistry(ctx context.Context) error {
	if c.raft == nil || c.raft.State() != raft.Leader {
		return errNotLeader
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.verificationMu.Lock()
	verification := c.verification
	if verification == nil {
		verification = &registryVerification{done: make(chan struct{})}
		c.verification = verification
		go func() {
			verification.err = c.raft.VerifyLeader().Error()
			c.verificationMu.Lock()
			c.verification = nil
			close(verification.done)
			c.verificationMu.Unlock()
		}()
	}
	c.verificationMu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return net.ErrClosed
	case <-verification.done:
		if errors.Is(verification.err, raft.ErrNotLeader) || errors.Is(verification.err, raft.ErrLeadershipLost) {
			return errNotLeader
		}
		return verification.err
	}
}
