# Cluster reliability todo list

Priorities from the 2026-10-26 overload benchmarks. Evidence and workload details are in
[the benchmark summary](benchmark-results/2026-10-26-saturation/summary.json).

- [ ] **Handle subscriber quota exhaustion reliably for QoS 1 and 2.**
  - Define the delivery policy when a subscriber reaches `MaximumInflight` (8,192 by default).
  - Prevent accepted messages from disappearing from subscriber delivery when the quota is reached. Evaluate bounded backpressure, durable queuing, and explicit failure handling, including cross-node forwarding.
  - Add regression coverage for quota exhaustion, slow subscribers, recovery, and duplicate delivery. The overload run lost 570 QoS 2 deliveries after publisher handshakes completed; the same quota path applies to QoS 1.

- [ ] **Expose subscriber quota losses in metrics.**
  - [x] Export `InflightDropped` through Prometheus, `/stats`, and `$SYS`, alongside outbound queue and cluster forwarding drops. Include the new counter deltas in benchmark reporting; missing fields from older brokers are `null`.
  - Expose queue usage, quota limits, and saturation events without unbounded client-ID labels.
  - Verify that reported losses reconcile with unique subscriber delivery counts. The existing forwarding and `$SYS` drop counters did not report the QoS 2 quota losses.

- [ ] **Resolve intermittent registry claim failures.**
  - [x] Reproduce and trace the failures. Registry replies were lost between the worker interface and broker pod while Kindnet NetworkPolicy enforcement was active. See the [registry trace report](benchmark-results/2026-10-26-registry-trace/REPORT.md).
  - [x] Compare configurations: the original policy produced 16 CONNECT refusals in 36,000 attempts; disabling it produced zero in 18,000, and permitting all ingress between broker peers produced zero in another 18,000. The original policy has been restored.
  - [x] Narrow the loss to established reply packets without Kindnet policy labels, with netlink receive-buffer pressure. A socket-only A/B/A produced 218/18,000 refusals with the original buffer, 0/18,000 enlarged, and 117/18,000 after restoration. Deploy the startup buffer patch and repeat churn with the original restrictive policy: 54,000 attempts passed, including an agent restart. See the [validation report](benchmark-results/2026-10-26-reliability-validation/REPORT.md).
  - [ ] Resolve upstream NFQUEUE fail-open and unlabelled established-reply handling. The local receive-buffer patch mitigates the reproduced loss; it does not guarantee policy enforcement under arbitrary overload. Evaluate a supported upstream fix without broadening the broker policy.
  - [x] Verify low-load client admission after a leader change as well as saturation. With the CNI buffer patch, both rolling transitions passed 1,000 admission/subscription attempts and all 9,000 expected QoS 2 deliveries after each transition. The leader changed from broker 0 to broker 2. This covers the previously failing low-load scenario, not every possible failure mode.
  - [x] Preserve underlying RPC errors, request type, peer, phase, and duration through terminal proposal failures. Expose remote RPC attempt, failure, and timeout counters; retain `errors.Is(err, ErrNoQuorum)` compatibility.
  - [x] Gate `/healthz` on known local voter membership, Raft commit catch-up, and a bounded active registry check. Check the admission RPC path on followers and quorum on the leader, with first-voter bootstrap and mixed-version upgrade compatibility.
  - [x] Exercise the new readiness and diagnostics in an isolated local cluster: bootstrap, blocked registry RPC with healthy Raft, recovery, and real old/new mixed-version rolling transitions passed. The full Go race suite and focused regression repetitions passed.
  - [ ] Cover the case where a registry mutation commits but its reply is lost, including claim cleanup, safe retries, and fencing. Consider connection reuse to reduce registry TCP churn after checking protocol and retry semantics.
  - [ ] Make rolling replacement safe when pod IPs change and peer discovery is disrupted. Restoring logging stalled the rollout: the surviving voter tried an old peer IP, while replacements had empty Raft stores. Require stable, caught-up voter membership before advancing the rollout and provide an address-recovery path that can operate without an existing leader. See the trace report's restoration section.
