#! /bin/sh

rm -rf ./build/app/docs 

mkdir -p ./build/app/docs
mkdir -p ./build/airbyte-repository

# check if the repository has been cloned already
if [ -d "./build/airbyte-repository/.git" ]; then
    cd ./build/airbyte-repository
    git reset --hard
    git clean -dfx
    git pull --depth=1
    cd ../..
else
    git clone --depth=1 https://github.com/airbytehq/airbyte.git build/airbyte-repository
fi

cp -R build/airbyte-repository/docs/integrations ./build/app/docs/integrations
cp -R build/airbyte-repository/docs/.gitbook ./build/app/docs/.gitbook
[ -f "./build/app/docs/integrations/sources/google-ads.md" ] || { ls -Rlha ./docs; echo "::error ::Failed to copy docs from airbyte repository"; exit 1; }
mv ./build/app/docs/integrations/sources/google-ads.md ./build/app/docs/integrations/sources/gglad.md
