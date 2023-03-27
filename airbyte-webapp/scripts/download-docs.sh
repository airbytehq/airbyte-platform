#! /bin/sh

rm -rf ./build/app/docs ./build/airbyte-repository

git clone --depth=1 https://github.com/airbytehq/airbyte.git build/airbyte-repository

mkdir -p ./build/app/docs
cp -R build/airbyte-repository/docs/integrations ./build/app/docs/integrations
cp -R build/airbyte-repository/docs/.gitbook ./build/app/docs/.gitbook
[ -f "./build/app/docs/integrations/sources/google-ads.md" ] || { ls -Rlha ./docs; echo "::error ::Failed to copy docs from airbyte repository"; exit 1; }
mv ./build/app/docs/integrations/sources/google-ads.md ./build/app/docs/integrations/sources/gglad.md

rm -rf ./build/airbyte-repository