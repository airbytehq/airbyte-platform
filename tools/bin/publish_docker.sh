#!/bin/bash
set -e

# List of directories without "airbyte-" prefix.
projectDir=(
  "bootloader"
  "config/init"
  "connector-sidecar"
  "container-orchestrator"
  "cron"
  "connector-builder-server"
  "db/db-lib"
  "metrics/reporter"
  "proxy"
  "server"
  "temporal"
  "webapp"
  "workers"
  "workload-api-server"
  "workload-launcher"
  "init-container"
  "keycloak"
  "keycloak-setup"
)

# Set default values to required vars. If set in env, values will be taken from there.
# Primarily for testing.
JDK_VERSION=${JDK_VERSION:-21.1.0}
ALPINE_IMAGE=${ALPINE_IMAGE:-alpine:3.18}
POSTGRES_IMAGE=${POSTGRES_IMAGE:-postgres:13-alpine}

# Iterate over all directories in list to build one by one.
# metrics-reporter are exception due to wrong artifact naming
for workdir in "${projectDir[@]}"
  do
    case $workdir in
      "config/init")
        artifactName="init"
        ;;

      "metrics/reporter")
        artifactName="metrics-reporter"
        ;;

      "workers")
        artifactName="worker"
        ;;

      *)
        artifactName=${workdir%/*}
        ;;
    esac

    echo "Publishing airbyte/$artifactName..."
    sleep 1

    docker buildx create --use --driver=docker-container --name $artifactName && \
    docker buildx build -t "airbyte/$artifactName:$VERSION" \
      --platform linux/amd64,linux/arm64                    \
      --build-arg VERSION=$VERSION                          \
      --build-arg ALPINE_IMAGE=$ALPINE_IMAGE                \
      --build-arg POSTGRES_IMAGE=$POSTGRES_IMAGE            \
      --build-arg JDK_VERSION=$JDK_VERSION                  \
      --sbom=true                                           \
      --push                                                \
      airbyte-$workdir/build/airbyte/docker
    docker buildx rm $artifactName
done
