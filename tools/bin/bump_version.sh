#!/usr/bin/env bash
set -eu

set -o xtrace
REPO=$(git ls-remote --get-url | xargs basename -s .git)
echo $REPO

GIT_REVISION=$(git rev-parse HEAD)

pip install bumpversion
if [ -z "${OVERRIDE_VERSION:-}" ]; then
  # No override, so bump the version normally
  bumpversion --current-version "$PREV_VERSION" "$PART_TO_BUMP"
else
  # We have an override version, so use it directly
  bumpversion --current-version "$PREV_VERSION" --new-version $OVERRIDE_VERSION "$PART_TO_BUMP"
fi

if [ "$REPO" == "airbyte" ]; then
  NEW_VERSION=$(grep -w 'VERSION=[0-9]\+\(\.[0-9]\+\)\+' run-ab-platform.sh | cut -d"=" -f2)
  echo "Bumped version for Airbyte"
else
  NEW_VERSION=$(grep -w VERSION .env | cut -d"=" -f2)
  echo "Bumped version for Airbyte Platform"
fi

export VERSION=$NEW_VERSION # for safety, since lib.sh exports a VERSION that is now outdated

set +o xtrace
echo "Bumped version from ${PREV_VERSION} to ${NEW_VERSION}"
echo "PREV_VERSION=${PREV_VERSION}" >> $GITHUB_OUTPUT
echo "NEW_VERSION=${NEW_VERSION}" >> $GITHUB_OUTPUT
