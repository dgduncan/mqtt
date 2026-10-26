# Registry failure trace — 2026-10-26

The intermittent CONNECT refusals reproduced in the local Docker Kubernetes cluster originate in the network path used by the registry RPCs. The evidence isolates Kindnet NetworkPolicy enforcement and connection tracking as the area to fix. The exact kernel or policy-engine defect remains unproven.

## What was traced

The three brokers ran the existing `mochi-cluster:auth-fix` image, one per worker, with the same resources and a five-second registry claim timeout. Broker debug logging was temporarily enabled. The in-cluster load generator ran on the control-plane node. Each churn attempt opened an MQTT connection, subscribed, and disconnected using a unique client ID.

Sixteen churn runs attempted 96,000 connections and subscriptions in total, with 107 CONNECT refusals (`0x88`) and no subscription-stage failures. One run used 128 workers; the rest used 256. Broker claim-failure counters attributed 102 failures to broker 0 and five to broker 1. Broker 2 remained the registry leader in the captured failures.

Debug logs exposed the underlying error as a TCP **response read timeout** to the leader on port 7947. The application then returned the generic `cluster session registry unavailable` / `ErrNoQuorum` error and refused the MQTT connection. That terminal error alone cannot establish quorum loss.

## Packet evidence

Simultaneous captures on worker and pod interfaces matched 16 failed registry RPCs in the `boundary-trace-0` run: ten claims and six releases. For every one:

1. The TCP connection completed and the leader received the request.
2. The leader sent a successful registry response, which reached the caller's worker interface within **0.62–1.72 ms** of the start of that TCP connection.
3. The response and its early retransmissions did not reach the caller's pod interface before the five-second deadline.
4. The caller timed out and closed the connection. A later response retransmission reached the pod at about 6.38 seconds, after the call had failed.

For example, `10.244.1.6:52312 → 10.244.3.23:7947` was a claim. Its successful response reached the caller's worker at 1.659 ms. The first captured response inside the caller's pod arrived at 6,378.769 ms. The broker logged a read timeout at `2026-10-26T17:36:13.288252543Z`.

The response carries the registry protocol's success status after its Raft apply path. These calls therefore show successful registry operations whose replies were lost on their way back to the caller. The captured delay was outside the registry handler.

The analysis separates repeated TCP connection instances: this workload can reuse a source port, so grouping only by IP addresses and ports would mix older successful replies into a later failed call.

Evidence: [per-call summary](boundary-summary.json), [packet timelines](failed-rpc-boundary.json), and `boundary-{node,pod}-{0,2}.pcap.gz`. Captures were limited to registry/cluster ports 7947 and 7948, with a 96-byte snapshot length. The analysis scripts read compressed captures directly.

## Configuration comparison

Each row used three runs of 6,000 attempts with 256 concurrent workers. No broker restart or image/resource change occurred between these comparison runs.

| Configuration, in execution order | Attempts | CONNECT refusals | Subscription failures | Worker conntrack insert failures / drops |
| --- | ---: | ---: | ---: | ---: |
| Original chart NetworkPolicy | 18,000 | 14 | 0 | 1,108 / 1,108 |
| Chart NetworkPolicy disabled | 18,000 | 0 | 0 | 0 / 0 |
| Original chart NetworkPolicy restored | 18,000 | 2 | 0 | 1,057 / 1,057 |
| Policy enabled, extra ingress rule allowing all ports between broker peers | 18,000 | 0 | 0 | 844 / 844 |

The disabled-policy runs completed around 13,100–13,500 connection/subscription cycles per second, with p99 latency around 40–42 ms. Original-policy runs ranged around 709–2,773 cycles per second, with p99 around 1.05–2.32 seconds. These are observations of this shared local environment, not production capacity estimates.

The policy-disabled control removes both the refusals and the conntrack insert/drop increments. Broadening only peer ingress removes the observed refusals while retaining conntrack failures and high tail latency. Together with the packet captures, this supports a return-traffic classification/state problem in the policy path. It does not identify the exact failing instruction or prove that every conntrack drop causes a refusal; TCP recovers many packet losses.

The original chart policy already allows broker peers to reach TCP 7947 and 7948. Legitimate reply traffic should be handled as part of those connections. The temporary extra peer rule was a diagnostic control and has been removed.

Ordinary interface, qdisc, IP error/discard, and Kubernetes `KUBE-FORWARD` invalid-drop counters did not explain the missing responses. The native nftables `kindnet-network-policies` table sends selected pod traffic through NFQUEUE 101 and uses connection labels to bypass policy evaluation for established traffic. That table is not fully represented by `iptables-save`.

Evidence: [policy comparison with raw counters](policy-comparison.json), [peer-reply comparison](reply-policy-comparison.json), [all results](results.jsonl), and `mochi-cluster-*-policy-debug.log` / `mochi-cluster-*-reply-policy-debug.log`.

## Remediation and remaining uncertainty

- Investigate or update the local CNI/policy implementation, and rerun churn with the original restrictive policy enabled. Leave that policy enabled for normal operation.
- Improve broker diagnostics so a registry transport read timeout retains its cause, operation, peer and elapsed time, instead of only surfacing `ErrNoQuorum`.
- Exercise successful registry mutations whose replies are lost. Retry and cleanup behavior must preserve session ownership and fencing. Connection reuse may reduce TCP churn, but needs a protocol design and correctness coverage.
- Capture a policy verdict or kernel drop reason for an affected TCP instance to identify the exact upstream defect. The current trace locates the loss and shows configuration dependence; it does not establish a specific upstream bug or validate a CNI upgrade as the fix.

The installed network image was `docker.io/kindest/kindnetd:v20260528-9350166c`. Its [pinned dependency manifest](https://github.com/kubernetes-sigs/kind/blob/9350166c/images/kindnetd/go.mod) uses `kube-network-policies v1.0.1-0.20260528073847-f67f0fb35e2b`. The [corresponding dataplane source](https://github.com/kubernetes-sigs/kube-network-policies/blob/f67f0fb35e2b/pkg/dataplane/controller.go) shows the NFQUEUE and connection-label handling. Upstream later fixed a [separate documented queue-loss mechanism during nftables table rebuilds](https://github.com/kubernetes-sigs/kube-network-policies/issues/402) in [v1.1.2](https://github.com/kubernetes-sigs/kube-network-policies/releases/tag/v1.1.2). That is a candidate to consider when selecting an update, not a confirmed explanation for these captured failures.

## Additional failure during restoration

Returning logging from debug to info required a rolling pod replacement. That rollout stalled with all three brokers unready. Replacement broker 2 had changed from `10.244.3.23` to `10.244.3.24`, and broker 1 from `10.244.2.6` to `10.244.2.7`. Gossip membership became disrupted. The surviving broker 0 repeatedly attempted Raft elections against broker 1's **old** address, `10.244.2.6:7948`; replacement brokers had empty in-memory Raft stores and could not elect themselves.

This is an additional implementation recovery gap. `reconcileRaft()` updates voter addresses only on an existing leader, leaving an address-change deadlock when no leader can reach the stored addresses. The rollout had also advanced to another replacement before the registry and membership remained stable. The initiating race and contribution of the policy failure need further isolation; the observed stale-address deadlock is recorded directly in the logs.

Recovery preserved broker 0's in-memory registry. A temporary, narrowly scoped DNAT rule on its worker mapped the stale Raft address to the replacement pod. Broker 0 won an election immediately, installed its existing snapshot on broker 1, and committed broker 1's current address. The rule was then removed. The final rollout step was temporarily held while isolated broker 2 rejoined and received a snapshot. Once all three voters were present, the final step completed. No registry reset or cluster rebootstrap was performed.

Evidence: [stalled elections](restore-broker-0.log), [successful recovery and snapshot replication](recovery-broker-0.log), and [final status](restored-status.json). `temporary-recovery.nft` preserves the diagnostic rule used; it is no longer installed. The StatefulSet rollout partition was returned to zero.

After restoration all three pods reported ready and the policy specification exactly matched the original. However, the first direct in-cluster QoS 2 smoke check [timed out connecting a subscriber to broker 2](restored-qos2-error.txt), before publishing began. A single retry then [received a CONNECT refusal `0x88` on the same broker](restored-qos2-retry-error.txt), again before publishing. A host port-forward monitor also timed out during MQTT setup. Neither check produced a QoS 2 delivery measurement. Readiness and recovered Raft leadership therefore do not establish that network reliability is fixed. Low-load admission after a leader change is included in the open remediation work.

## Reproduction

Use the local `docker-desktop` context and the supplied saturation load-generator image/pod manifest. All scripts explicitly select `/Users/dduncan/.kube/config` and namespace `mochi`.

- `policy_comparison.py` runs the original-policy / disabled / restored comparison and restores `networkPolicy.enabled=true` in its cleanup block.
- `reply_policy_comparison.py` adds the temporary peer-ingress rule, runs three churn samples, and restores the original ingress rules in its cleanup block.
- `run_boundary.py` captures worker and pod interfaces around a churn sample. Its saved pod PIDs and sniffer executable placement must be refreshed after redeployment. `capture_tcp.go.txt` contains the Linux ARM64 capture source.
- `analyze_boundary.py` and `analyze_packets.py` correlate debug errors and captured frames. Debug logging must be enabled for those errors to appear.

Normal info logging and the original chart NetworkPolicy were restored after tracing. Temporary load-generation and packet-capture resources were removed. The findings do not implement the quota-handling, loss-metrics, or registry-remediation work in the [todo list](../../TODO.md).
