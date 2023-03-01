#!/usr/bin/env bash

# todo (cgardens) - de-dupe this with the one that is in oss (tool/bin/check_images_exist.sh). only lines 17 and 24 are different. we don't have a clean way to pass bash scripts from OSS to cloud.bu

set -e

. tools/lib/lib.sh

function docker_tag_exists() {

    name="$1"
    tag="$2"
    curatedName=$( sed -E 's/"//g' <<<"$name" )
    curatedTag=$( sed -E 's/"//g' <<<"$tag" )
    URL=https://hub.docker.com/v2/repositories/"$curatedName"/tags/"$curatedTag"
    printf "\tURL: %s\n" "$URL"
    curl --silent -f -lSL -H "Authorization: JWT $3" "$URL" > /dev/null
}

checkPlatformImages() {
  echo "Checking platform images exist..."
  docker compose -f docker-compose.build.yaml pull || exit 1
  echo "Success! All platform images exist!"
}

checkImagesByName() {
  DOCKER_HUB_TOKEN=$(curl -s -H "Content-Type: application/json" -X POST -d '{"username": "'${DOCKER_HUB_USER}'", "password": "'${DOCKER_HUB_PASSWORD}'"}' https://hub.docker.com/v2/users/login/ | jq -r .token)

  while [ "$#" -gt "0" ]; do
    ARG="$1" ; shift

    IFS=":" read -r REPO TAG <<< "${ARG}"

    # Skip non-Aribyte images:
    [[ ! "${REPO}" =~ ^airbyte/ ]] && continue

    if docker_tag_exists "$REPO" "$TAG" "$DOCKER_HUB_TOKEN"; then
        printf "\tSTATUS: found\n"
    else
        printf "\tERROR: not found!\n" && exit 1
    fi
  done
}

checkConnectorImages() {
  echo "Checking connector images exist..."

  # unfortunately the API does not allow using access tokens. we have to user username and password to fetch a JWT.
  DOCKER_HUB_TOKEN=$(curl -s -H "Content-Type: application/json" -X POST -d '{"username": "'${DOCKER_HUB_USER}'", "password": "'${DOCKER_HUB_PASSWORD}'"}' https://hub.docker.com/v2/users/login/ | jq -r .token)

  CONNECTOR_DEFINITIONS=$(grep "dockerRepository" -h -A1 airbyte-bootloader-wrapped/src/main/resources/seed/*.yaml | grep -v -- "^--$" | tr -d ' ')
  [ -z "CONNECTOR_DEFINITIONS" ] && echo "ERROR: Could not find any connector definition." && exit 1

  while IFS=":" read -r _ REPO; do
      IFS=":" read -r _ TAG
      printf "${REPO}: ${TAG}\n"
      if docker_tag_exists "$REPO" "$TAG" "$DOCKER_HUB_TOKEN"; then
          printf "\tSTATUS: found\n"
      else
          printf "\tERROR: not found!\n" && exit 1
      fi
  done <<< "${CONNECTOR_DEFINITIONS}"

  echo "Success! All connector images exist!"
}

main() {
  assert_root

  SUBSET=${1:-all} # default to all.

  shift

  [[ ! "$SUBSET" =~ ^(all|platform|connectors|byname)$ ]] && echo "Usage ./tools/bin/check_image_exists.sh [all|platform|connectors|byname]" && exit 1

  echo "checking images for: $SUBSET"

  echo "authenticating with docker hub"

  [[ "$SUBSET" =~ ^(all|platform)$ ]] && checkPlatformImages

  [[ "$SUBSET" =~ ^(all|connectors)$ ]] && checkConnectorImages

  [ "$SUBSET" == "byname" ] && checkImagesByName $@

  echo "Image check complete."
}

main "$@"
