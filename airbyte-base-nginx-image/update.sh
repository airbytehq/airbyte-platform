#!/bin/bash

git clone https://github.com/nginxinc/docker-nginx-unprivileged.git
git -C docker-nginx-unprivileged co 1.27.3

rm -rf ./src
mkdir src
cp -r docker-nginx-unprivileged/mainline/alpine-slim/* ./src
