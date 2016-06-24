#!/bin/bash -xe

docker run \
-w /digdag \
-v `pwd`/:/digdag \
-v ~/.gradle:/root/.gradle \
digdag-build \
./gradlew clean test --info --no-daemon
