#! /bin/bash

# Script that calculates the hash of all input files that go into generating the webapp build output.
# This script isn't perfect. It doesn't cover ALL input that affects the build output e.g. it's not taking
# environment variables into account. However that's fine since the hash is only needed to determine if the
# app had an update, to show a notification. The check doesn't need to be 100% perfect, since it's fine if
# we miss some minor update that only had a change in environment variables.
# Therefore this hash should NEVER be used as a real 100% accurate hash of the output.

HASHES="$(find src public pnpm-lock.yaml -not -name buildInfo.json -type f -print | xargs openssl sha1 -binary | xxd -p)"

# Build a single hash over all the hashes of all files and export it for usage in other scripts
export SOURCE_HASH="$(echo "${HASHES}" | openssl sha1 -binary | xxd -p)"

