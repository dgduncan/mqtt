#!/usr/bin/env bash
# Build the local Kindnet receive-buffer fix against the installed upstream commits.
# Requires Git, Go >= 1.26, Docker, and public dependency access.
set -euo pipefail

repo_dir="$(cd "$(dirname "$0")/../.." && pwd)"
work_dir="$(mktemp -d /tmp/mochi-kindnet-build.XXXXXX)"
trap 'rm -rf "$work_dir"' EXIT
task_arch="${KINDNET_ARCH:-$(go env GOARCH)}"
task_image="${KINDNET_IMAGE:-kindnetd:mochi-nfqueue-buffer}"
kind_commit=9350166c3dac18c25ff99f77df0811cef71178d8
policy_commit=f67f0fb35e2b8288782bc1f22988648580dda7f8

fetch_source() {
  local name="$1" commit="$2" destination="$3"
  git init -q "$destination"
  git -C "$destination" fetch --quiet --depth 1 "https://github.com/kubernetes-sigs/$name.git" "$commit"
  git -C "$destination" checkout --quiet --detach FETCH_HEAD
}
fetch_source kind "$kind_commit" "$work_dir/kind"
fetch_source kube-network-policies "$policy_commit" "$work_dir/policies"
git -C "$work_dir/policies" apply "$repo_dir/deploy/kind/kindnet-nfqueue-buffer.patch"

mkdir "$work_dir/context"
cd "$work_dir/kind/images/kindnetd"
go mod edit "-replace=sigs.k8s.io/kube-network-policies=$work_dir/policies"
CGO_ENABLED=0 GOOS=linux GOARCH="$task_arch" go build -trimpath -ldflags '-s -w' -o "$work_dir/context/kindnetd" ./cmd/kindnetd
cat > "$work_dir/context/Dockerfile" <<'DOCKERFILE'
FROM docker.io/kindest/kindnetd:v20260528-9350166c
COPY kindnetd /bin/kindnetd
LABEL org.opencontainers.image.description="Local NFQUEUE receive-buffer fix for the MQTT reliability lab"
DOCKERFILE
docker build --platform "linux/$task_arch" -t "$task_image" "$work_dir/context"
