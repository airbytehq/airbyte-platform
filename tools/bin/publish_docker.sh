#!/bin/bash
set -e

# List of directories without "airbyte-" prefix.
projectDir=(
  "bootloader"
  "config/init"
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
  "keycloak"
  "keycloak-setup"
  "api-server"
)

# Set default values to required vars. If set in env, values will be taken from there.
# Primarily for testing.
JDK_VERSION=${JDK_VERSION:-17.0.4}
ALPINE_IMAGE=${ALPINE_IMAGE:-alpine:3.14}
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

      "api-server")
        artifactName="airbyte-api-server"
        ;;

      *)
        artifactName=${workdir%/*}
        ;;
    esac

    case $workdir in
      "webapp")
        dockerDir="build/docker"
        ;;

      *)
        dockerDir="build/airbyte/docker"
        ;;
    esac

    echo "Publishing airbyte/$artifactName..."
    sleep 1

    docker buildx create --use --name $artifactName &&      \
    docker buildx build -t "airbyte/$artifactName:$VERSION" \
      --platform linux/amd64,linux/arm64                    \
      --build-arg VERSION=$VERSION                          \
      --build-arg ALPINE_IMAGE=$ALPINE_IMAGE                \
      --build-arg POSTGRES_IMAGE=$POSTGRES_IMAGE            \
      --build-arg JDK_VERSION=$JDK_VERSION                  \
      --push                                                \
      airbyte-$workdir/$dockerDir
    docker buildx rm $artifactName
done
