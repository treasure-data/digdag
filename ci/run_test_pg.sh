#!/bin/bash -xe

export DIGDAG_TEST_POSTGRESQL="
host = localhost
port = 5432
user = digdag_test
password =
database = digdag_test
idleTimeout = 5
minimumPoolSize = 0
"

docker run \
-w /digdag \
-v `pwd`/:/digdag \
-v ~/.gradle:/root/.gradle \
-e DIGDAG_TEST_POSTGRESQL="${DIGDAG_TEST_POSTGRESQL}" \
-e TD_API_KEY="${TD_API_KEY}" \
-e TD_LOAD_IT_SFTP_USER="${TD_LOAD_IT_SFTP_USER}" \
-e TD_LOAD_IT_SFTP_PASSWORD="${TD_LOAD_IT_SFTP_PASSWORD}" \
-e CI_NODE_TOTAL="${CI_NODE_TOTAL}" \
-e CI_NODE_INDEX="${CI_NODE_INDEX}" \
-e CI_ACCEPTANCE_TEST=true \
-e TERM=dumb \
$BUILD_IMAGE \
./gradlew clean cleanTest test --info --no-daemon "$@"
