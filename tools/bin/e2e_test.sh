#!/usr/bin/env bash

set -e

. tools/lib/lib.sh

assert_root

echo "Starting app..."

if [[ ! -z "$CI" ]]; then
    CI_MODE_FLAG='-DciMode=true'
fi

# todo (cgardens) - docker-compose 1.27.3 contained a bug that causes a failure if the volume path
#  does not exist when the volume is created. It was fixed in 1.27.4. Github actions virtual envs,
#  however, new ubuntu release upgraded to 1.27.3 on 09/24/20. Once github actions virtual envs
#  upgrades to 1.27.4, we can stop manually making the directory.
mkdir -p /tmp/airbyte_local

# Detach so we can run subsequent commands
VERSION=dev BASIC_AUTH_USERNAME="" BASIC_AUTH_PASSWORD="" TRACKING_STRATEGY=logging docker compose up -d

# When developing locally, we'll want to clean up the docker images we started each time
if [ "$CI" != true ]; then
  trap 'docker compose down && docker stop airbyte_ci_pg_source airbyte_ci_pg_destination dummy_api' EXIT
fi
# Uncomment for debugging; replaces the previous trap. Warning, this is verbose.
# trap 'echo "docker compose logs:" && docker compose logs -t --tail 1000 && docker compose down && docker stop airbyte_ci_pg_source airbyte_ci_pg_destination dummy_api' EXIT

docker run --rm -d -p 5433:5432 -e POSTGRES_PASSWORD=secret_password -e POSTGRES_DB=airbyte_ci_source --name airbyte_ci_pg_source postgres
docker run --rm -d -p 5434:5432 -e POSTGRES_PASSWORD=secret_password -e POSTGRES_DB=airbyte_ci_destination --name airbyte_ci_pg_destination postgres
# network name will be "${name-of-current-dir}_airbyte_internal"
docker run --rm -d -p 6767:6767 --mount type=bind,source="$(pwd)"/airbyte-webapp/scripts/dummy_api.js,target=/index.js --name=dummy_api node:16-alpine "index.js"

echo "Waiting for health API to be available..."
# Retry loading the health API of the server to check that the server is fully available
until $(curl --output /dev/null --fail --silent --max-time 5 --head localhost:8001/api/v1/health); do
  echo "Health API not available yet. Retrying in 10 seconds..."
  sleep 10
done

echo "Running e2e tests via gradle without cypress key"

if ./gradlew --no-daemon :airbyte-webapp:e2etest "$CI_MODE_FLAG"; then
  echo "Tests succeeded"
else
  echo "Tests failed, retry and record"
  ./gradlew --no-daemon :airbyte-webapp:e2etest -PcypressWebappKey=$CYPRESS_WEBAPP_KEY "$CI_MODE_FLAG";
fi

