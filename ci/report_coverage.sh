#!/bin/bash -xe

docker run \
-w /digdag \
-v `pwd`/:/digdag \
-v ~/.gradle:/root/.gradle \
-e TERM=dumb \
digdag-build \
./gradlew jacocoTestReport coveralls --info --no-daemon "$@"
