# Cluster reliability todo list

Priorities from the 2026-10-26 overload benchmarks. Evidence and workload details are in
[the benchmark summary](benchmark-results/2026-10-26-saturation/summary.json).

- [ ] **Handle subscriber quota exhaustion reliably for QoS 1 and 2.**
  - Define the delivery policy when a subscriber reaches `MaximumInflight` (8,192 by default).
  - Prevent accepted messages from disappearing from subscriber delivery when the quota is reached. Evaluate bounded backpressure, durable queuing, and explicit failure handling, including cross-node forwarding.
  - Add regression coverage for quota exhaustion, slow subscribers, recovery, and duplicate delivery. The overload run lost 570 QoS 2 deliveries after publisher handshakes completed; the same quota path applies to QoS 1.

- [ ] **Expose subscriber quota losses in metrics.**
  - Export `InflightDropped` alongside outbound queue drops and cluster forwarding drops.
  - Expose queue usage, quota limits, and saturation events without unbounded client-ID labels.
  - Verify that reported losses reconcile with unique subscriber delivery counts. The existing forwarding and `$SYS` drop counters did not report the QoS 2 quota losses.

- [ ] **Resolve intermittent registry claim failures.**
  - [x] Reproduce and trace the failures. Registry replies were lost between the worker interface and broker pod while Kindnet NetworkPolicy enforcement was active. See the [registry trace report](benchmark-results/2026-10-26-registry-trace/REPORT.md).
  - [x] Compare configurations: the original policy produced 16 CONNECT refusals in 36,000 attempts; disabling it produced zero in 18,000, and permitting all ingress between broker peers produced zero in another 18,000. The original policy has been restored.
  - [ ] Identify and remedy the precise Kindnet/conntrack defect, then repeat churn with the original policy enabled. Evaluate a supported CNI update; neither temporary policy change is a permanent fix. Successful registry replies reached the worker in 0.62–1.72 ms in the fully captured failures but missed the broker's five-second deadline.
  - [ ] Verify low-load client admission after a leader change as well as saturation. After restoration, two small QoS 2 checks failed during subscriber setup on broker 2 (one TCP timeout, one `0x88` refusal) despite all pods reporting ready.
  - [ ] Preserve the underlying RPC error and expose request type, peer, phase, duration, and timeout counters. Distinguish transport timeout from actual quorum loss; `ErrNoQuorum` currently hides the preceding read timeout.
  - [ ] Cover the case where a registry mutation commits but its reply is lost, including claim cleanup, safe retries, and fencing. Consider connection reuse to reduce registry TCP churn after checking protocol and retry semantics.
  - [ ] Make rolling replacement safe when pod IPs change and peer discovery is disrupted. Restoring logging stalled the rollout: the surviving voter tried an old peer IP, while replacements had empty Raft stores. Require stable, caught-up voter membership before advancing the rollout and provide an address-recovery path that can operate without an existing leader. See the trace report's restoration section.
