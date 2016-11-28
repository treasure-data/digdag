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
-e GCP_CREDENTIAL="${GCP_CREDENTIAL}" \
-e GCS_TEST_BUCKET="${GCS_TEST_BUCKET}" \
-e EMR_IT_S3_TEMP_BUCKET="${EMR_IT_S3_TEMP_BUCKET}" \
-e EMR_IT_AWS_ACCESS_KEY_ID="${EMR_IT_AWS_ACCESS_KEY_ID}" \
-e EMR_IT_AWS_SECRET_ACCESS_KEY="${EMR_IT_AWS_SECRET_ACCESS_KEY}" \
-e EMR_IT_AWS_ROLE="${EMR_IT_AWS_ROLE}" \
-e CI_NODE_TOTAL="${CI_NODE_TOTAL}" \
-e CI_NODE_INDEX="${CI_NODE_INDEX}" \
-e CI_ACCEPTANCE_TEST=true \
-e TERM=dumb \
$BUILD_IMAGE \
./gradlew clean cleanTest test --info --no-daemon "$@"
