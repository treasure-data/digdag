#!/bin/bash -xe

fakes3 -r tmp/fakes3 -p 19876 &
export FAKE_S3_ENDPOINT=http://127.0.0.1:19876

docker run \
--net=host \
-w /digdag \
-v `pwd`/:/digdag \
-v ~/.gradle:/root/.gradle \
-e FAKE_S3_ENDPOINT="${FAKE_S3_ENDPOINT}" \
digdag-build \
./gradlew clean test --info --no-daemon
