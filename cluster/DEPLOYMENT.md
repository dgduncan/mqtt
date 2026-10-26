# Deploying a cluster

A plan for running the cluster on Vultr, GCP and other clouds with as few manual steps as
possible. Everything after a one-time account setup is driven by CI/CD.

## Recommendation

Run the cluster on **managed Kubernetes on every cloud** (GKE on GCP, VKE on Vultr), from
**one container image and one Helm chart**, with infrastructure in **Terraform** and
deployments from **GitHub Actions**.

- Kubernetes gives each cloud the same primitives the cluster needs: stable node names
  (StatefulSet), discovery by DNS (headless Service), a TCP load balancer (Service of type
  `LoadBalancer`), readiness gating on `/healthz`, SIGTERM with a grace period for draining,
  and one-at-a-time rolling updates. The only per-cloud differences are a few Helm values
  and the Terraform module.
- Every change goes through the same pipeline: test, build the image once, deploy to
  staging, smoke test, promote the same image digest to production.
- What remains manual is a one-time setup per cloud (see [Manual steps](#manual-steps)).

Plain VMs (a GCE managed instance group, Vultr instances with cloud-init) also work, but
you have to build discovery, rolling restarts and health-gated deploys yourself. Serverless
container platforms (Cloud Run, App Runner) don't fit: they don't accept raw TCP or UDP
traffic, and they scale instances in and out freely.

## What the cluster needs from its environment

These follow from how the cluster works ([cluster.go](cluster.go), [registry.go](registry.go)):

| Need | Why | How on Kubernetes |
|---|---|---|
| Ports 1883/tcp (MQTT), 7946/tcp+udp (gossip), 7947/tcp (forwarding), 7948/tcp (raft), 8080/tcp (admin) | Each node listens on all of them | Container ports; only 1883 is exposed through the load balancer |
| A **private network** between nodes | Forwarding and raft links are plain TCP with no authentication; gossip is plaintext unless `memberlist.Config.SecretKey` is set | Pod network plus a NetworkPolicy that allows 7946–7948 only from other broker pods |
| Unique, stable node names | `NodeName` identifies raft voters | StatefulSet pod name (`mqtt-0`, `mqtt-1`, …) |
| Seeds | Joining needs at least one existing node | Headless Service DNS name, `mqtt-peers:7946`. memberlist resolves a hostname to all of its addresses and tries each one |
| Exactly one bootstrapping node | `Bootstrap` creates a new cluster only when no seed can be reached | Pod `mqtt-0` only; others never bootstrap |
| An odd number of nodes, 3 or 5, spread across zones | The session registry needs a raft quorum; 3 nodes tolerate 1 failure, 5 tolerate 2 | `replicas: 3`, topology spread across zones, PodDisruptionBudget `maxUnavailable: 1` |
| Never restart every node at once | The registry is kept in memory by default, so a node recovers it from the others; if all nodes restart together, session ownership is lost | OrderedReady rolling update gated on readiness; PDB |
| Graceful stop | SIGTERM drains the node (clients reconnect elsewhere and their sessions move) | `terminationGracePeriodSeconds` longer than the drain (the example waits up to 60s) |
| Health | `/healthz` checks local voter membership, Raft commit catch-up, registry quorum and the admission RPC path, and returns 503 on failure or while draining | **Readiness** probe on `/healthz`, so the load balancer stops sending clients to that node. **Liveness** and startup probes on `/livez` keep registry isolation from causing restarts |
| Load balancing | Any node accepts any client, and sessions follow the client | Layer 4 TCP load balancer, no session affinity |
| Metrics | `/metrics` serves Prometheus text | GKE: Managed Service for Prometheus `PodMonitoring`. VKE: kube-prometheus-stack or Grafana Cloud agent |

Cluster nodes should be in one region. Raft and claims (`ClaimTimeout`, `LeaseTimeout`) are
tuned for LAN latency. To serve several regions, run a separate cluster in each one, or bridge
them, rather than stretching one cluster across regions.

## In this repo

Stage 1 is built:

| Piece | Where | Notes |
|---|---|---|
| Node binary | [cmd/cluster](../cmd/cluster) | Every setting is a flag or an `MQTT_*` environment variable (`mochi-cluster -help` lists them). Clients are checked against an auth ledger file (`MQTT_AUTH_FILE`); allowing every client takes an explicit `MQTT_ALLOW_ALL=true`. Optional TLS listener (certificates reload when the files change), websocket listener, and gossip encryption (`MQTT_GOSSIP_KEY`). Admin http serves `/metrics`, `/stats`, `/healthz`, `/livez` and `/drain` |
| Image | [deploy/docker/Dockerfile.cluster](../deploy/docker/Dockerfile.cluster) | Distroless, non-root, about 21MB, cross-compiled for amd64 and arm64 |
| Helm chart | [deploy/helm/mochi-cluster](../deploy/helm/mochi-cluster) | StatefulSet, headless peer Service, client Service, PodDisruptionBudget, NetworkPolicy, auth and gossip key Secrets, PodMonitoring/ServiceMonitor. Refuses to render without auth configured |
| Smoke test | [deploy/kind/smoke.sh](../deploy/kind/smoke.sh) | Installs three nodes on kind, checks membership, sends QoS 1 messages between nodes with an authenticated client, checks a wrong password is refused, then does a rolling restart and checks again |
| CI | [.github/workflows/cluster.yml](../.github/workflows/cluster.yml) | Chart lint and schema validation, cluster unit tests, chaos suite (pushes, nightly, and pull requests touching the cluster or server), kind smoke test, and on pushes a multi-arch image pushed to `ghcr.io/<owner>/mochi-cluster`, tagged by commit, branch and version, with its digest in the run summary. The chart is also packaged and pushed to `ghcr.io/<owner>/charts/mochi-cluster` |

How nodes find each other: every node is given the headless Service's DNS name as its seed,
and resolves it itself, leaving out its own address. The library creates a new cluster when
it has no seeds, so this decides who may bootstrap. A node whose name ends in `-0` bootstraps
only if no other node resolves. Any other node waits up to `MQTT_JOIN_TIMEOUT` for one to
appear, then exits, and never starts a second cluster. The headless Service publishes nodes
which aren't ready yet (`publishNotReadyAddresses`), because a node can't become ready until
it has joined.

Probes: readiness is `/healthz`. The node must know its voter membership, have a nonzero
commit index, catch its Raft applied index up to the commit index observed by the probe,
remain in contact with the registry, and complete an active registry check within 750 ms
(or the HTTP request's earlier deadline). A leader verifies quorum; a follower checks
the leader over the same RPC listener used for admission. Failures and draining return
503, taking the node out of the client Service after the probe failure threshold.
Liveness and startup are `/livez`, so registry isolation does not cause restarts. The
startup probe allows for the join timeout.

The applied-index check uses Raft's public progress counters, not a guarantee that the
application FSM has finished every queued operation. Stable voter membership, durable
Raft storage and recovery from stale peer addresses remain separate reliability work.

The check permits a single bootstrapped voter, allowing OrderedReady startup to create
the remaining pods. During a rolling upgrade, a new follower sends an impossible
ownership confirmation (empty client ID, epoch zero). An older leader processes that
confirmation through Raft without changing ownership; a new leader uses a read-only
quorum verification. New-version probes therefore work before the leader is upgraded.
`/stats`'s `InContact` and `mochi_cluster_registry_in_contact` describe Raft lease contact;
they do not include this active readiness check.

### Local Kindnet network loss

For the Docker Desktop lab, see the [Kindnet receive-buffer mitigation](../deploy/kind/README.md)
and [validation report](../benchmark-results/2026-10-26-reliability-validation/REPORT.md).
The original restrictive NetworkPolicy remains enabled; the local CNI patch
addresses reproduced registry refusals under connection churn. It does not change
subscriber quota handling or Raft persistence.

### Delivery losses and registry diagnostics

The cluster's `/metrics` endpoint includes these node-wide metrics, without client-ID
labels. `/stats` exposes the corresponding fields for benchmark snapshots.

| Prometheus metric | `/stats` field | Meaning |
| --- | --- | --- |
| `mochi_cluster_subscriber_inflight_dropped_total` | `SubscriberInflightDropped` | Existing `InflightDropped` counter: subscriber quota/packet-ID failures and expired messages discarded during session restoration |
| `mochi_cluster_subscriber_messages_dropped_total` | `SubscriberMessagesDropped` | Subscriber outbound queue drops, separate from cluster forwarding drops |
| `mochi_cluster_subscriber_inflight_messages` | `SubscriberInflight` | Current stored inflight messages across all clients on this node |
| `mochi_cluster_subscriber_inflight_limit` | `SubscriberInflightLimit` | Configured inflight storage limit **per client**; it is not a node-wide capacity |
| `mochi_cluster_registry_rpc_calls_total` | `RegistryRPCCalls` | Remote registry RPC attempts, including readiness checks; excludes local Raft operations |
| `mochi_cluster_registry_rpc_failures_total` | `RegistryRPCFailures` | Remote RPC errors, including transport/protocol errors and remote rejections |
| `mochi_cluster_registry_rpc_timeouts_total` | `RegistryRPCTimeouts` | Remote RPC attempts whose local context or transport timed out |

The broker also publishes the inflight-drop count at the retained
`$SYS/broker/messages/inflight/dropped` topic on its normal system-topic interval.
This count includes session-restore expirations, so it is not exclusively a quota-loss
counter. The benchmark runner in `benchmark-results/2026-10-26-saturation/run.py`
records these new counter deltas; missing fields from older brokers are recorded as
`null`, not zero.

Registry RPC failures retain the operation, peer address, phase (`connect`, `deadline`,
`write`, `read`, or `response`), elapsed duration, and underlying error. A terminal
proposal failure preserves both the context error and last RPC failure's cause while
remaining compatible with `errors.Is(err, cluster.ErrNoQuorum)`. Inspect the wrapped
cause before attributing that sentinel to actual quorum loss. Individual attempts and
response-write failures are logged at debug level; admission warnings include the final
wrapped cause at the configured normal log level.

Auth files loaded by `cmd/cluster` end with an automatic catch-all ACL deny. This applies
both to Helm's inline `auth.ledger` and to `auth.existingSecret`. Authentication alone no
longer grants topic access: give clients explicit publish and subscribe permissions. For
example:

```yaml
auth:
  ledger:
    auth:
      - username: device
        password: replace-with-a-secret
        allow: true
    acl:
      - username: device
        filters:
          "devices/#": 3  # read and write
      - filters:
          "#": 0        # deny all other topics
```

When upgrading an existing authenticated deployment, add the topic grants before deploying
the new image. An existing ledger containing only authentication rules will still accept
valid credentials but will deny all publishing and subscriptions. Put specific restrictions
before broad allow rules. The explicit development option `auth.allowAll: true` continues
to allow all clients and topics when no auth file is configured.

Still to do:

1. **Terraform** in `deploy/terraform/{gcp,vultr}` for the Kubernetes cluster, registry, DNS
   and static IP, and per-cloud values files (`values-gcp.yaml`, `values-vultr.yaml`).
2. **Deploy jobs**: staging on pushes to the default branch, production on tags (see
   [CI/CD](#cicd)), deploying the image digest the `image` job publishes.
3. **Optional raft persistence** to a volume (raft-boltdb), so that restarting every node at
   once doesn't lose the session registry.
4. **Encrypt and authenticate peer links.** Forwarding and raft are plaintext, so the cluster
   relies on the private network and the NetworkPolicy. mTLS on both listeners (a
   `tls.Config` option in `cluster.Options`) would remove that dependency, and would allow
   node pools in different networks.

## Installing the published chart

Every push to a default branch and every `v*` tag packages the chart and pushes it to GHCR as
an OCI artifact. A tag publishes its own version (`v1.4.2` becomes `1.4.2`, with `appVersion`
set to match, as the image for that tag is), while a branch push publishes a snapshot named
`<chart version>-sha<short commit>`, so snapshots never collide with a release:

```sh
helm install mochi-cluster oci://ghcr.io/<owner>/charts/mochi-cluster \
  --version 1.4.2 -n mqtt --create-namespace -f my-values.yaml
```

The package GHCR creates is private. Anonymous pulls need it made public once, under *Package
settings → Change visibility* for the `charts/mochi-cluster` package; otherwise log in with a
token that has `read:packages`:

```sh
helm registry login ghcr.io -u <user>   # personal access token, read:packages
```

The release version is not checked against `Chart.yaml`, so bump `version` there when you tag
if you want a checkout and a published chart to describe themselves the same way.

## GCP

| Piece | Choice |
|---|---|
| Kubernetes | GKE **Autopilot**, regional (nodes spread across three zones, no node pools to manage) |
| Images | Artifact Registry (or GHCR) |
| Load balancer | `Service` type `LoadBalancer`, which gives a regional passthrough Network Load Balancer. Reserve a static IP in Terraform and set `loadBalancerIP` |
| DNS | Cloud DNS record such as `mqtt.example.com` pointing to the static IP |
| TLS for clients | Terminate in the broker (cert-manager with Let's Encrypt, mounted as a secret), because the passthrough NLB doesn't terminate TLS |
| Metrics | Managed Service for Prometheus with `PodMonitoring` on port `admin` |
| CI credentials | **Workload Identity Federation**: GitHub's OIDC token is exchanged for a short-lived service account token, so no long-lived keys are stored in GitHub |
| Terraform state | GCS bucket |

If you prefer VMs:
a regional managed instance group on Container-Optimized OS, with seeds found from instance
metadata or a DNS name, and an internal health check on `/healthz`. That route takes more
custom work for rolling updates.

## Vultr

| Piece | Choice |
|---|---|
| Kubernetes | **VKE** (Vultr Kubernetes Engine), 3 nodes in one region, attached to a VPC |
| Images | Vultr Container Registry, or GHCR |
| Load balancer | `Service` type `LoadBalancer`; Vultr's cloud controller creates a Vultr Load Balancer. Set annotations for TCP protocol and the health check, for example `service.beta.kubernetes.io/vultr-loadbalancer-protocol: tcp` |
| DNS | Vultr DNS, or your existing DNS provider, through Terraform |
| TLS for clients | cert-manager in the broker, as on GCP |
| Metrics | kube-prometheus-stack in the cluster, or a Grafana Cloud agent |
| CI credentials | Vultr API key as a GitHub Environment secret (Vultr has no OIDC federation). Use a dedicated key, and rotate it. The kubeconfig is fetched with the key in each run, not stored |
| Terraform state | Vultr Object Storage (S3-compatible backend) |

Vultr regions don't have separate availability zones, so a VKE cluster sits in one location
(enable the HA control plane option). For higher availability, run a second cluster in
another region.

VM alternative: three instances in a VPC, created by Terraform, with cloud-init installing
Docker and a systemd unit running the image. Seeds are the other instances' private IPs from
Terraform. Deploys then need a rolling script over SSH that restarts one instance and waits
for `/healthz` before moving on. It works, but it's what Kubernetes already does for you.

## Other clouds

The same chart runs anywhere with managed Kubernetes and a TCP load balancer: AWS EKS (NLB),
Azure AKS, DigitalOcean DOKS, Linode LKE, Hetzner (k3s plus the hcloud cloud controller). Each
needs only its own Terraform module and a values file. Fly.io also supports UDP and a private
network between machines, but would need a different deploy path.

## CI/CD

```
PR ──► vet, lint, race tests ──► chaos suite (cluster/ changes) ──► build image (no push)
       helm lint + kubeconform ──► terraform plan (posted as a PR comment)

main ──► build + push image (tag: sha, digest recorded)
     ──► package + push helm chart to GHCR (snapshot: <chart version>-sha<commit>)
     ──► deploy to staging: GCP and Vultr in parallel (helm upgrade --atomic --wait)
     ──► smoke test: loadtest cross-node through each staging load balancer
     ──► terraform apply (staging)

tag v* ──► promote the same digest to production (GitHub Environment "production")
       ──► helm upgrade --atomic --wait, one cloud at a time
       ──► smoke test; --atomic rolls back on failure

nightly ──► loadtest benchmark against staging, compared with the last run
        ──► full chaos suite
```

- **Tests.** The existing `build.yml` already runs `go vet` and `go test -race ./...`. Add
  the chaos suite (`cd cluster/chaos && go test -tags chaos -timeout 30m`). GitHub's
  `ubuntu-latest` runners have Docker, so it runs as is. Run it on PRs that change `cluster/`,
  and nightly.
- **Build once.** Build the image only on `main`, push it to GHCR with
  `docker/build-push-action` (amd64 and arm64), and deploy by **digest**, so staging and
  production run identical bits. The `docker` job in `build.yml` publishes the server image to
  `ghcr.io/dgduncan/mqtt` on version tags.
- **Deploy.** `helm upgrade --install --atomic --wait --timeout 15m`. With OrderedReady and the
  readiness probe, Kubernetes restarts one pod at a time, waits for it to rejoin and report
  healthy, and only then moves on. `--atomic` rolls back if any pod doesn't become ready.
- **Smoke test.** Run [loadtest](../examples/benchmark/loadtest) publishing on one pod and
  subscribing on another (port-forward to two pods, or target the load balancer twice), and
  fail the deploy if delivery is below 100% at QoS 1. Also check `/stats` on each pod for the
  expected member count.
- **Approvals.** Production is a GitHub Environment. Leave it fully automatic, or add one
  required reviewer as a single click. That's the only manual step in a release.
- **Infrastructure.** Terraform `plan` on PRs that change `deploy/terraform`, `apply` on merge,
  with the production apply behind the same environment protection.
- **Option: GitOps.** If more clusters are added, install Argo CD or Flux in each cluster and
  have CI only push the image and commit the new digest to a `deploy/` values file. Each
  cluster then pulls its own changes, and CI holds no cluster credentials. For two clouds,
  pushing from Actions is simpler to start with.

## Manual steps

One time, per cloud:

1. Create the account or project and enable billing.
2. **GCP:** create the Workload Identity pool and provider for this GitHub repository, plus
   the deploy service account. This can be a small bootstrap Terraform run locally once.
   **Vultr:** create an API key and save it as a GitHub Environment secret.
3. Create the Terraform state bucket.
4. Point the DNS zone's nameservers at Cloud DNS or Vultr DNS, if they manage it.
5. Add the MQTT auth configuration and gossip key as secrets (or let Terraform create them in
   Secret Manager).

After that, infrastructure changes, deploys, rollbacks and benchmark runs all go through
pull requests and tags.

## Rollout order

1. ~~Repo work: `cmd/cluster`, image, chart, CI with a kind smoke test.~~ Done (stage 1).
   Next: Terraform modules and the deploy jobs.
2. GCP staging, end to end through CI.
3. Vultr staging with the same chart.
4. Production on one cloud, then the second, each with its own DNS name. Optionally, put
   weighted or failover DNS records across the two.
