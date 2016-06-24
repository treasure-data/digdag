#!/bin/bash -ex

if [[ -e ~/docker/digdag-build.tar.gz ]]; then
    gunzip -c ~/docker/digdag-build.tar.gz | docker load
fi
cd "$(dirname "$0")"
docker build -t digdag-build .
mkdir -p ~/docker/
docker save digdag-build | gzip -c > ~/docker/digdag-build.tar.gz
