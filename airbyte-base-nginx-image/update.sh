#!/bin/bash

git clone https://github.com/nginxinc/docker-nginx-unprivileged.git
git -C docker-nginx-unprivileged checkout ecb8070 # mainline 1.31.8; upstream stopped tagging releases

rm -rf ./src
mkdir src
cp -r docker-nginx-unprivileged/mainline/alpine-slim/* ./src
