// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2026 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package cluster

import (
	"bufio"
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

// rpcFixture can withhold a reply without disrupting gossip or Raft traffic.
func rpcFixture(t *testing.T, respond func(net.Conn, byte, []byte)) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var connections []net.Conn
	accepted := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(accepted)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			connections = append(connections, conn)
			mu.Unlock()
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer conn.Close()
				typ, body, err := readFrame(bufio.NewReader(conn))
				if err == nil {
					respond(conn, typ, body)
				}
			}()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		<-accepted
		mu.Lock()
		for _, conn := range connections {
			_ = conn.Close()
		}
		mu.Unlock()
		wg.Wait()
	})
	return ln.Addr().String()
}

func followerAndLeader(t *testing.T, nodes []*node) (*Cluster, *Cluster) {
	t.Helper()
	var follower, leader *Cluster
	for _, n := range nodes {
		if n.cluster.raft.State() == raft.Leader {
			leader = n.cluster
		} else {
			follower = n.cluster
		}
	}
	require.NotNil(t, follower)
	require.NotNil(t, leader)
	return follower, leader
}

func redirectRegistry(t *testing.T, c *Cluster, leader, addr string) {
	t.Helper()
	c.peersMu.RLock()
	p := c.peers[leader]
	c.peersMu.RUnlock()
	require.NotNil(t, p)
	old := p.address()
	p.setAddr(addr)
	t.Cleanup(func() { p.setAddr(old) })
}

func TestReadinessBootstrapAndDrain(t *testing.T) {
	c := newCluster(t, "bootstrap-0")[0].cluster
	require.Eventually(t, func() bool { return c.CheckReady(context.Background()) == nil }, waitFor, tick)
	c.draining.Store(true)
	require.ErrorIs(t, c.CheckReady(context.Background()), ErrDraining)
	c.draining.Store(false)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, c.CheckReady(ctx), context.Canceled)
}

func TestReadinessIsReadOnly(t *testing.T) {
	nodes := newCluster(t, "n1", "n2", "n3")
	follower, leader := followerAndLeader(t, nodes)
	require.Eventually(t, func() bool { return follower.CheckReady(context.Background()) == nil }, waitFor, tick)
	before := leader.raft.LastIndex()
	for range 5 {
		require.NoError(t, follower.CheckReady(context.Background()))
	}
	require.Equal(t, before, leader.raft.LastIndex(), "new peers must verify quorum without appending probe commands")
	_, exists := leader.registry.owner("")
	require.False(t, exists)
}

func TestReadinessDetectsLostRPCReplyWithHealthyRaft(t *testing.T) {
	nodes := newCluster(t, "n1", "n2", "n3")
	follower, leader := followerAndLeader(t, nodes)
	addr := rpcFixture(t, func(conn net.Conn, _ byte, _ []byte) { var b [1]byte; _, _ = conn.Read(b[:]) })
	redirectRegistry(t, follower, leader.Name(), addr)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := follower.CheckReady(ctx)
	require.Error(t, err)
	require.Less(t, time.Since(started), time.Second)
	require.True(t, follower.inContact(), "Raft reachability must not conceal a broken admission path")
	var rpcErr *RegistryRPCError
	require.ErrorAs(t, err, &rpcErr)
	require.Equal(t, "read", rpcErr.Phase)
	require.Equal(t, addr, rpcErr.Peer)
	require.EqualValues(t, 1, follower.Stats().RegistryRPCTimeouts)
}

func TestReadinessLegacyLeaderConfirmation(t *testing.T) {
	nodes := newCluster(t, "n1", "n2", "n3")
	follower, leader := followerAndLeader(t, nodes)
	var confirmations atomic.Int64
	addr := rpcFixture(t, func(conn net.Conn, typ byte, body []byte) {
		if typ != frameConfirmRequest {
			return
		}
		id, rest, err := readString(body)
		if err != nil || id != "" {
			return
		}
		name, _, err := readString(rest)
		if err != nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		result, err := leader.applyLocal(ctx, encodeConfirm(id, name, 0))
		if err != nil || result != false {
			return
		}
		confirmations.Add(1)
		_, _ = conn.Write(encodeFrame(frameResponse, []byte{statusOK, 0}))
	})
	redirectRegistry(t, follower, leader.Name(), addr)
	require.NoError(t, follower.CheckReady(context.Background()))
	require.EqualValues(t, 1, confirmations.Load())
	_, exists := leader.registry.owner("")
	require.False(t, exists)
}

func TestRegistryProposalPreservesRPCError(t *testing.T) {
	nodes := newCluster(t, "n1", "n2", "n3")
	follower, leader := followerAndLeader(t, nodes)
	addr := rpcFixture(t, func(conn net.Conn, _ byte, _ []byte) { var b [1]byte; _, _ = conn.Read(b[:]) })
	redirectRegistry(t, follower, leader.Name(), addr)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := follower.claim(ctx, "lost-reply")
	require.ErrorIs(t, err, ErrNoQuorum)
	var rpcErr *RegistryRPCError
	require.ErrorAs(t, err, &rpcErr)
	require.Equal(t, "claim", rpcErr.Operation)
	require.Equal(t, "read", rpcErr.Phase)
	require.Equal(t, addr, rpcErr.Peer)
	require.Positive(t, rpcErr.Elapsed)
	var netErr net.Error
	require.True(t, errors.Is(err, context.DeadlineExceeded) || errors.As(err, &netErr) && netErr.Timeout())
	s := follower.Stats()
	require.EqualValues(t, 1, s.RegistryRPCCalls)
	require.EqualValues(t, 1, s.RegistryRPCFailures)
	require.EqualValues(t, 1, s.RegistryRPCTimeouts)
}

func TestRegistryRPCCancellation(t *testing.T) {
	c := newCluster(t, "n1")[0].cluster
	addr := rpcFixture(t, func(conn net.Conn, _ byte, _ []byte) { var b [1]byte; _, _ = conn.Read(b[:]) })
	ctx, cancel := context.WithCancel(context.Background())
	timer := time.AfterFunc(30*time.Millisecond, cancel)
	defer timer.Stop()
	defer cancel()
	_, err := c.call(ctx, addr, frameConfirmRequest, []byte{0})
	require.ErrorIs(t, err, context.Canceled)
	var rpcErr *RegistryRPCError
	require.ErrorAs(t, err, &rpcErr)
	require.Equal(t, "read", rpcErr.Phase)
	require.Zero(t, c.Stats().RegistryRPCTimeouts, "cancellation is not a timeout")
}
