#!/bin/bash

version=$1

if [ -z "${version}" ]; then
  echo "Usage:  ./update-mirrored-image.sh <Keycloak Docker image version>"
  exit -1
else
  echo "Updating mirrored Keycloak image to version '$version'..."

  # Update AMD version of the image
  docker pull --platform linux/amd64 quay.io/keycloak/keycloak:$version
  docker tag quay.io/keycloak/keycloak:$version airbyte/mirrored-keycloak-amd:$version
  docker push airbyte/mirrored-keycloak-amd:$version

  # Update ARM version of the image
  docker pull --platform linux/arm64 quay.io/keycloak/keycloak:$version
  docker tag quay.io/keycloak/keycloak:$version airbyte/mirrored-keycloak-arm:$version
  docker push airbyte/mirrored-keycloak-arm:$version

  # Create a multi-arch manifest. This will create a new image `airbyte/mirrored-keycloak:21.1` that will be a manifest of the two images above.
  docker manifest create airbyte/mirrored-keycloak:$version \
    --amend airbyte/mirrored-keycloak-amd:$version \
    --amend airbyte/mirrored-keycloak-arm:$version

  # Annotate the manifest with the two architectures. This tells Docker which image is associated with what architecture.
  docker manifest annotate airbyte/mirrored-keycloak:$version airbyte/mirrored-keycloak-amd:$version --arch amd64
  docker manifest annotate airbyte/mirrored-keycloak:$version airbyte/mirrored-keycloak-arm:$version --arch arm64

  # Publish the manifest for the mirrored Docker image
  docker manifest push airbyte/mirrored-keycloak:$version

  echo "Airbyte mirrored version of Keycloak Docker image has been successfully updated to version '$version'."
fi