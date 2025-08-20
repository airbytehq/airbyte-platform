#!/bin/bash

# Clean only gitignored files from the generated directories. This prevents outdated generated typescript files from
# sticking around in the repository, which could cause type errors when our build/local dev commands run.
echo "Cleaning gitignored files from generated directories..."
git clean -fX src/core/api/generated/
git clean -fX src/core/api/types/
echo "Generated directories cleanup complete."