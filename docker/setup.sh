#!/bin/bash -ex

TARBALL_EXISTS=false

if [[ -e ~/docker/digdag-build.tar.gz ]]; then
    TARBALL_EXISTS=true
    gunzip -c ~/docker/digdag-build.tar.gz | docker load
fi

IMAGE_ID_PRE=$(docker inspect --format="{{.Id}}" digdag-build)

cd "$(dirname "$0")"
docker build -t digdag-build .
mkdir -p ~/docker/

IMAGE_ID_POST=$(docker inspect --format="{{.Id}}" digdag-build)

echo IMAGE_ID_PRE: $IMAGE_ID_PRE
echo IMAGE_ID_POST: $IMAGE_ID_POST

if [[ $TARBALL_EXISTS == false || $IMAGE_ID_PRE != $IMAGE_ID_POST ]]; then
    docker save digdag-build | gzip -c > ~/docker/digdag-build.tar.gz
fi

