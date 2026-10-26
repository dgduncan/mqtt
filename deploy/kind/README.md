# Local Kindnet receive-buffer fix

The Docker Desktop lab's Kindnet image
`docker.io/kindest/kindnetd:v20260528-9350166c` intermittently refused MQTT
connections during client churn with NetworkPolicy enabled. Increasing only its
NFQUEUE netlink receive buffer removed the reproduced failures. See the
[validation report](../../benchmark-results/2026-10-26-reliability-validation/REPORT.md).

`kindnet-nfqueue-buffer.patch` sets the buffer at startup, checks the effective
size, and logs it. On this Linux kernel a 4 MiB request produces an 8 MiB effective
socket buffer. This preserves the installed NetworkPolicy rules, NFQUEUE queue
length, fail-open setting, and Kindnet CPU/memory settings. It is a local patch
against the installed source commits, not an upstream release or a general fix
for every source of packet loss.

## Build

Requires Git, Go 1.26 or later, Docker, and public dependency access. The script
fetches the pinned Kind and kube-network-policies commits into a temporary
directory, applies the patch, and replaces the binary in the original image.

```sh
./deploy/kind/build-kindnet-buffer.sh
```

The default image is `kindnetd:mochi-nfqueue-buffer`, for the host's Go architecture.
Override `KINDNET_ARCH` and `KINDNET_IMAGE` when needed.

## Apply to the Docker Desktop lab

These commands deliberately select the local Kubernetes context. The node list
must match the Docker containers backing that cluster. Save the original
DaemonSet before the first change.

```sh
kubectl --context=docker-desktop -n kube-system get ds kindnet -o yaml > /tmp/kindnet-before.yaml
docker save -o /tmp/kindnet-buffer.tar kindnetd:mochi-nfqueue-buffer
for node in desktop-control-plane desktop-worker desktop-worker2 desktop-worker3; do
  docker cp /tmp/kindnet-buffer.tar "$node:/var/mochi-kindnet-buffer.tar"
  docker exec "$node" ctr -n k8s.io images import /var/mochi-kindnet-buffer.tar
  docker exec "$node" rm /var/mochi-kindnet-buffer.tar
done
kubectl --context=docker-desktop -n kube-system set image ds/kindnet kindnet-cni=kindnetd:mochi-nfqueue-buffer
kubectl --context=docker-desktop -n kube-system rollout status ds/kindnet
kubectl --context=docker-desktop -n kube-system logs -l app=kindnet --prefix --tail=30
```

Confirm every agent logs `Configured NFQUEUE receive buffer` with
`bytes=8388608` in this lab, then repeat client churn and policy-denial checks.
A CNI restart can briefly interrupt networking; roll out one agent at a time.
The image must be loaded again after recreating the Docker Desktop cluster.

## Roll back

```sh
kubectl --context=docker-desktop -n kube-system set image ds/kindnet kindnet-cni=docker.io/kindest/kindnetd:v20260528-9350166c
kubectl --context=docker-desktop -n kube-system rollout status ds/kindnet
```

The original image and DaemonSet configuration are also recorded in the
validation evidence. No broader allow rules or conntrack flushes are required.

## Remaining limits

The NFQUEUE queue remains bounded and fail-open remains enabled upstream. Larger
buffers reduce the reproduced receive-buffer pressure; they do not guarantee
policy enforcement or loss-free admission at arbitrary load. An upstream fix
should address policy behavior when the first packet bypasses userspace and
its established reply has no acceptance label. Broker subscriber quota handling
and Raft recovery from stale peer addresses are separate open issues.
