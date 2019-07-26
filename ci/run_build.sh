#!/bin/bash -xe

docker run \
-v /var/run/docker.sock:/var/run/docker.sock \
-v /tmp:/tmp:rw \
-w /digdag \
-v `pwd`/:/digdag \
-v ~/.gradle:/root/.gradle \
-e TERM=dumb \
$BUILD_IMAGE \
./gradlew clean cli testClasses --info --no-daemon "$@"
