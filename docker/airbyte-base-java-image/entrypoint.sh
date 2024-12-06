#!/usr/bin/env bash

set -e

update-ca-trust

exec "$@"
