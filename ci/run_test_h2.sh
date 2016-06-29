#!/bin/bash -xe

docker run \
-w /digdag \
-v `pwd`/:/digdag \
-v ~/.gradle:/root/.gradle \
-e TD_API_KEY="${TD_API_KEY}" \
digdag-build \
./gradlew clean test --info --no-daemon
