#!/bin/bash -xe

export DIGDAG_TEST_POSTGRESQL="
host = localhost
port = 5432
user = digdag_test
password =
database = digdag_test
"

docker run \
-w /digdag \
-v `pwd`/:/digdag \
-v ~/.gradle:/root/.gradle \
-e DIGDAG_TEST_POSTGRESQL="${DIGDAG_TEST_POSTGRESQL}" \
-e TD_API_KEY="${TD_API_KEY}" \
digdag-build \
sh -c "/etc/init.d/postgresql start && ./gradlew test --info --no-daemon"
