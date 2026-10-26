# Reliability validation and Kindnet investigation — 2026-10-26

## Results

- `go test -race ./...` passed, including the new readiness, RPC error/cancellation,
  subscriber-loss metric and `$SYS` regressions. Focused regressions also passed
  three consecutive runs with the race detector. Helm lint passed.
- The isolated three-voter broker release bootstrapped successfully. Blocking only
  a follower's admission RPC egress made `/healthz` return 503 in 0.761 seconds,
  while its Raft `InContact` stayed true. Removing the block restored readiness.
- A Kindnet receive-buffer mitigation removed the reproduced registry refusals
  with the original restrictive NetworkPolicy enabled. The persistent startup
  patch passed 36,000 connection/subscription attempts at concurrency 256 and 512,
  followed by another 18,000 at concurrency 512 after restarting one agent and
  18,000 against the main broker cluster: **72,000 attempts, zero failures**.
- All 9,000 expected QoS 2 deliveries arrived after the patch, with no duplicates.
  This was 3,000 publications, three subscribers and approximately 2,995 publishes/s;
  delivery p99 was 2.163 ms. It does not exercise subscriber quota exhaustion.

## What failed

The earlier investigation placed registry replies between the worker interface and
broker pod. This run narrowed that down further:

1. Kindnet debug logs showed explicit ingress denials on client ephemeral ports,
   alongside netlink receive errors (`no buffer space available`).
2. A read-only nftables trace filtered to replies from the validation leader's
   registry port. The observed replies were `ESTABLISHED`, in conntrack's reply
   direction; they were not invalid, new, or untracked.
3. Some established replies lacked Kindnet's policy acceptance label. They entered
   NFQUEUE again and were denied as ingress to the ephemeral destination port.
4. Accepting only established reply-direction packets between validation broker
   IPs removed the failures in A/B/A comparisons. This diagnostic rule was removed.
5. Increasing only the existing NFQUEUE netlink socket buffers removed the
   failures, without restarting agents or changing policy, CPU, or queue length.
   Restoring the original buffer brought the failures back.

The installed effective receive buffer was 212,992 bytes. A 4 MiB request gives
8,388,608 effective bytes on this kernel. The socket-only comparison was:

| Same workload and agents | Connection attempts | CONNECT failures |
| --- | ---: | ---: |
| Original buffer | 18,000 | 218 |
| Enlarged buffer | 18,000 | 0 |
| Original buffer restored | 18,000 | 117 |

Each phase comprised three runs of 6,000 clients at concurrency 256, with a
subscription per successful connection. The workload reached roughly 1,000
connections/s; these are churn measurements, not steady message throughput.

Kindnet configures NFQUEUE with fail-open enabled. The kernel can accept a packet
when netlink delivery fails without the userspace controller applying its policy
label. Combined with the observed ENOBUFS, missing labels, and socket-only A/B/A,
receive-buffer pressure causing fail-open bypass is the strongly supported
explanation. We did not instrument the kernel's fail-open branch for each packet.
Queue-drop and conntrack-drop counters alone did not expose these failures.
See the [upstream controller](https://github.com/kubernetes-sigs/kube-network-policies/blob/f67f0fb35e2b8288782bc1f22988648580dda7f8/pkg/dataplane/controller.go)
and [kernel NFQUEUE implementation](https://github.com/torvalds/linux/blob/master/net/netfilter/nfnetlink_queue.c).

Increasing Kindnet's CPU allocation from 100m to 1 CPU removed CPU throttling and
improved tail latency, but did not remove refusals. That comparison ran with
verbose packet logging, which itself changes load; it should not be used as a
capacity comparison against the original logging configuration.

## Local mitigation

The [source patch](../../deploy/kind/kindnet-nfqueue-buffer.patch) configures and
verifies the receive buffer at startup. The [build script](../../deploy/kind/build-kindnet-buffer.sh)
uses the exact installed Kind and kube-network-policies commits, preserves the
original dependency versions, and replaces the Kindnet binary in its original
base image. The script was executed successfully end to end.

All four local Kindnet agents now run `kindnetd:mochi-nfqueue-buffer`. Each logged
and independently reported an 8,388,608-byte effective receive buffer. One worker
agent was deleted and recreated; the new process also configured that buffer.
Original CPU/memory settings (100m CPU, 50 MiB) and normal logging were preserved.

The 36,000-attempt patched run had **zero registry RPC failures or timeouts**
across 48,046 measured remote RPC attempts, including readiness traffic. No new
ENOBUFS appeared in agent logs. Subscriber inflight/outbound loss counters stayed
zero during this workload. After the agent restart, another 18,000 admission and
subscription attempts completed without failures.

Policy checks after the persistent patch confirmed:

- MQTT port 1883 remained reachable from the external test pod.
- Registry port 7947 remained blocked from that pod.
- A new connection from a broker pod's network namespace to unauthorized peer
  port 1884 remained blocked (timeout, rather than a connection-refused response).

No broad allow rule is installed. Temporary nftables trace/accept rules and the
RPC-blocking NetworkPolicy were removed. Build, deployment and rollback steps are
in [the Kindnet README](../../deploy/kind/README.md).

## Rolling upgrades and leader changes

The disposable validation release rolled from `reliability` to `auth-fix` and
back to `reliability`, taking 13.7 and 15.9 seconds respectively. Snapshots show
ready old and new pods together in both transitions. Broker 0 was initially the
leader; broker 2 became leader during the first transition. Pod IPs changed.
After each transition, 1,000 admission/subscription attempts passed and all 9,000
expected QoS 2 deliveries arrived with no duplicates. See
[the timeline and outcomes](rolling-validation.json). These successful transitions
do not prove recovery when a quorum is already unavailable.

## Main cluster and final deployment state

The main `mochi-cluster` release also passed 18,000 admission/subscription
attempts at concurrency 512 with zero failures. Its QoS 2 run delivered all 9,000
expected messages without duplicates, with p99 2.114 ms. All three brokers were
Ready with zero container restarts. See [the main-cluster results](main-cluster-validation.json).

The main brokers still run `mochi-cluster:auth-fix`; readiness/metrics/RPC changes
were validated in the disposable `reliability` release. The CNI mitigation is
active for the entire local cluster. The final DaemonSet container differs from
the original only in its image, and the main NetworkPolicy spec is identical to
the saved original. All four agents were Ready and their logs contained no
ENOBUFS. [Final state](final-state-before-cleanup.json) records the effective
buffers, policy comparison, rules and health checks.

The disposable broker release/namespace, load-generator pod, and node inspection
helpers were removed after testing. The patched CNI image remains deployed.
The [cleanup log](cleanup.log) records removal. Rollback commands are in the
Kindnet README.

## Limits and remaining work

This is a local, bounded mitigation, not proof of loss-free operation at every
load. NFQUEUE remains bounded and fail-open remains enabled. The policy handling
of established replies without an acceptance label needs an upstream resolution;
a larger buffer alone is not a guarantee against policy bypass under extreme load.
No upstream release was assumed to fix this particular issue.

Subscriber quota exhaustion remains open. Reporting losses and detecting registry
isolation do not implement subscriber backpressure or durable delivery. Raft is
still in memory; recovery from stale peer addresses when a quorum is unavailable
remains separate work.

## Evidence

- [Full race suite](go-test-race-final.log), [focused repetitions](focused-tests.log),
  [chart lint](helm-lint.log).
- [Readiness/RPC block test](live-validation.json).
- [Conntrack reply-state trace](reply-state-summary.json),
  [failed-claim correlations](failed-claim-state-evidence.json), compressed raw
  `reply-state-*.log.gz` and corresponding broker/Kindnet logs.
- [Socket-only A/B/A](socket-buffer-comparison.json),
  [CPU comparison](kindnet-cpu-comparison.json),
  [temporary established-reply comparison](reply-mitigation-comparison-normal.json).
- [Persistent patch validation](kindnet-patched-validation.json),
  [restart validation](kindnet-restart-validation.json),
  [policy checks](mitigation-isolation-persistent-patch.json),
  [reproducible build](kindnet-reproducible-build.log).

The socket-inspection helper is preserved as `netlink_buffer.go.txt` so it is not
part of the broker Go packages. It uses Linux pidfd/socket access and needs local
node privileges. The A/B/A scripts use saved PIDs and IPs and must be adapted to a
new lab; they are investigation artifacts, not general deployment tooling.
