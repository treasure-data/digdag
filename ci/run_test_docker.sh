#!/bin/bash -xe

docker run \
-v /var/run/docker.sock:/var/run/docker.sock \
-v /tmp:/tmp:rw \
-w /digdag \
-v `pwd`/:/digdag \
-v ~/.gradle:/root/.gradle \
-e DIGDAG_TEST_POSTGRESQL="${DIGDAG_TEST_POSTGRESQL}" \
-e DIGDAG_TEST_REDIS="${DIGDAG_TEST_REDIS}" \
-e TD_API_KEY="${TD_API_KEY}" \
-e TD_LOAD_IT_S3_BUCKET="${TD_LOAD_IT_S3_BUCKET}" \
-e TD_LOAD_IT_AWS_ACCESS_KEY_ID="${TD_LOAD_IT_AWS_ACCESS_KEY_ID}" \
-e TD_LOAD_IT_AWS_SECRET_ACCESS_KEY="${TD_LOAD_IT_AWS_SECRET_ACCESS_KEY}" \
-e GCP_CREDENTIAL="${GCP_CREDENTIAL}" \
-e GCS_TEST_BUCKET="${GCS_TEST_BUCKET}" \
-e EMR_IT_S3_TEMP_BUCKET="${EMR_IT_S3_TEMP_BUCKET}" \
-e EMR_IT_AWS_ACCESS_KEY_ID="${EMR_IT_AWS_ACCESS_KEY_ID}" \
-e EMR_IT_AWS_SECRET_ACCESS_KEY="${EMR_IT_AWS_SECRET_ACCESS_KEY}" \
-e EMR_IT_AWS_ROLE="${EMR_IT_AWS_ROLE}" \
-e EMR_IT_AWS_KMS_KEY_ID="${EMR_IT_AWS_KMS_KEY_ID}" \
-e REDSHIFT_IT_CONFIG="${REDSHIFT_IT_CONFIG}" \
-e CI_NODE_TOTAL="${CI_NODE_TOTAL}" \
-e CI_NODE_INDEX="${CI_NODE_INDEX}" \
-e CI_ACCEPTANCE_TEST="${CI_ACCEPTANCE_TEST:-true}" \
-e TEST_S3_ENDPOINT=$TEST_S3_ENDPOINT \
-e TEST_S3_ACCESS_KEY_ID=$TEST_S3_ACCESS_KEY_ID \
-e TEST_S3_SECRET_ACCESS_KEY=$TEST_S3_SECRET_ACCESS_KEY \
-e TERM=dumb \
$BUILD_IMAGE \
./gradlew clean cleanTest test --info --stacktrace --no-daemon "$@"
