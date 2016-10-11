#!/bin/bash -ex
BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $BASEDIR
REV=$(git rev-parse HEAD)
TS=$(date -u +'%Y%m%d%H%M%SZ')
TAG="${TS}-${REV}"
STATUS=$(git status --porcelain --ignore-submodules=dirty 2> /dev/null | tail -n1)
if [[ -n $STATUS ]]; then
	TAG="${TAG}-dirty"
fi
# TODO: namespace should be digdag or td
# NAMESPACE="digdag"
NAMESPACE="danieln"
NAME="digdag-build"
IMAGE="${NAMESPACE}/${NAME}:${TAG}"

docker build -t $IMAGE .
docker push $IMAGE
