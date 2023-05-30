#!/usr/bin/env bash

set -e

. tools/lib/lib.sh 

# Get the path to the Docker socket exposed by colima
if [[ $(docker context show) == "colima-ab-control-plane" ]]; then
    export DOCKER_HOST=$(colima --profile ab-control-plane status 2>&1 | grep socket | sed 's/^.*socket:\(.*\)"/\1/'g | tr -d '[:space:]')
    export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
    export LOCAL_VM=true
fi

# Build OSS
echo -e "\n${BLUE}Building OSS artifacts...${RESET}"
./gradlew assemble
