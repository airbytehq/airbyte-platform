#! /bin/sh

# Script that calculates the hash of all git tracked files as well as the files generated by orval

# Calculate the hash of all files we gitignored in /src/core/request i.e. all the auto generated Orval files.
GENERATED_CLIENT_HASHES="$(cat .gitignore | grep '/src/core/request' | xargs -I{} openssl sha1 .{})"
if [ -z "${GENERATED_CLIENT_HASHES}" ]; then
  # Fail if we can't find any file, so we don't accidentally forget to adjust the above path in case
  # we ever move the auto generated files to a different folder
  echo "Could not find any orval generated file to calculated hash."
  exit 25
fi

# Hash of all files tracked by git
SOURCE_HASHES="$(git ls-files | xargs openssl sha1)"

# On CI output all hashes for better debugging
if [ -n "${CI}" ]; then
  echo "# Calculated build hashes:"
  printf "${GENERATED_CLIENT_HASHES}\n${SOURCE_HASHES}"
  echo ""
fi

# Build a single hash over all the hashes of all files and export it for usage in other programs
export SOURCE_HASH="$(printf "${GENERATED_CLIENT_HASHES}\n${SOURCE_HASHES}" | openssl sha1 | sed 's/  -//')"
