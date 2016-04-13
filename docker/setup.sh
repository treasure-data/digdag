#!/bin/bash -ex

if [[ -e ~/docker/build.tar ]]; then
	docker load -i ~/docker/build.tar
fi
cd "$(dirname "$0")"
docker build -t digdag-build .
mkdir -p ~/docker/
docker save -o ~/docker/build.tar digdag-build
