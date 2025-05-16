#!/bin/bash
set -xeuo pipefail

version=$1

if [ -z "${version}" ]; then
  echo "Usage:  ./update-mirrored-image.sh <Keycloak Docker image version>"
  exit 1
fi

echo "Updating mirrored Keycloak image to version '$version'..."

# Create and push a multi-arch image directly by mirroring the existing one
docker buildx imagetools create \
  --tag airbyte/mirrored-keycloak:$version \
  quay.io/keycloak/keycloak:$version

docker buildx imagetools inspect airbyte/mirrored-keycloak:$version

echo "✅ Airbyte mirrored Keycloak Docker image updated to version '$version'."
