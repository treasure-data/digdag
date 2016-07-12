#!/bin/bash -xe

docker run \
-w /digdag \
-v `pwd`/:/digdag \
-v ~/.gradle:/root/.gradle \
-e TD_API_KEY="${TD_API_KEY}" \
-e TD_LOAD_IT_SFTP_USER="${TD_LOAD_IT_SFTP_USER}" \
-e TD_LOAD_IT_SFTP_PASSWORD="${TD_LOAD_IT_SFTP_PASSWORD}" \
-e CI_NODE_TOTAL="${CI_NODE_TOTAL}" \
-e CI_NODE_INDEX="${CI_NODE_INDEX}" \
-e CI_ACCEPTANCE_TEST=true \
-e TERM=dumb \
digdag-build \
./gradlew clean test --info --no-daemon
