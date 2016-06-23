#!/bin/bash -xe

docker run \
--net=host \
-w /digdag \
-v `pwd`/:/digdag \
-v ~/.gradle:/root/.gradle \
-e FAKE_S3_ENDPOINT="${FAKE_S3_ENDPOINT}" \
digdag-build \
./gradlew clean test --info --no-daemon
