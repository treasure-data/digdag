#!/bin/bash -xe

export DIGDAG_TEST_POSTGRESQL="
host = localhost
port = 5432
user = digdag_test
password =
database = digdag_test
"

docker run \
--net=host \
-w /digdag \
-v `pwd`/:/digdag \
-v ~/.gradle:/root/.gradle \
-e FAKE_S3_ENDPOINT="${FAKE_S3_ENDPOINT}" \
-e DIGDAG_TEST_POSTGRESQL="${DIGDAG_TEST_POSTGRESQL}" \
digdag-build \
sh -c "/etc/init.d/postgresql start && ./gradlew test --info --no-daemon"
