#!/usr/bin/env bash

[[ $(git rev-parse --is-inside-work-tree > /dev/null 2>&1) ]] || exit 0
# Get the current core.hooksPath config
hooksDir="$(git config core.hooksPath)"

# Check if the core.hooksPath already is set to the correct path
if [[ "$hooksDir" != *"airbyte-webapp/.githooks" ]]; then
  # Find the relative path of the airbyte-webapp folder inside the repository.
  # We don't need to cd out of the `scripts` folder for this, since this runs as
  # a prepare script which will be executed with airbyte-webapp/ as the current directory.
  hooksPath="$(git rev-parse --show-prefix).githooks"
  echo "Setting git core.hooksPath to ${hooksPath}"
  git config core.hooksPath "${hooksPath}"
fi
