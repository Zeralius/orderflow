#!/usr/bin/env bash
# OrderFlow on a local Kubernetes cluster, in one command:
#   builds the jars and images, starts a kind cluster (if not running), loads the images into it
#   and installs (or upgrades) the Helm chart. Needs: JDK 25, Docker, kind, Helm.
#
#   deploy/kind.sh            build, start, install
#   deploy/kind.sh --delete   remove the cluster again
set -euo pipefail
cd "$(dirname "$0")/.."

CLUSTER=orderflow
NAMESPACE=orderflow
APPS=(gateway order-service inventory-service payment-service notification-service)

if [[ "${1:-}" == "--delete" ]]; then
  kind delete cluster --name "$CLUSTER"
  exit 0
fi

# A new tag for every build, so Kubernetes rolls the pods even when only local changes differ
TAG=${TAG:-$(git rev-parse --short HEAD)$(git diff --quiet HEAD -- . ':!deploy' || echo "-dev$(date +%s)")}

if ! kind get clusters | grep -qx "$CLUSTER"; then
  kind create cluster --config deploy/kind/cluster.yaml
fi

echo "Building jars and images (tag $TAG)"
./mvnw -q -B package -DskipTests
IMAGES=()
for app in "${APPS[@]}"; do
  docker build -q -t "orderflow/$app:$TAG" "$app" > /dev/null
  IMAGES+=("orderflow/$app:$TAG")
done
kind load docker-image --name "$CLUSTER" "${IMAGES[@]}"

helm upgrade --install orderflow deploy/helm/orderflow \
  --namespace "$NAMESPACE" --create-namespace \
  --set image.tag="$TAG" \
  --wait --timeout 10m

helm test orderflow --namespace "$NAMESPACE" --logs
