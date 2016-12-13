#!/bin/bash -xe

docker run \
  -w /digdag/digdag-ui \
  -v `pwd`/:/digdag \
  -v ~/.gradle:/root/.gradle \
  -e TD_API_KEY="${TD_API_KEY}" \
  -e TD_LOAD_IT_SFTP_USER="${TD_LOAD_IT_SFTP_USER}" \
  -e TD_LOAD_IT_SFTP_PASSWORD="${TD_LOAD_IT_SFTP_PASSWORD}" \
  -e GCP_CREDENTIAL="${GCP_CREDENTIAL}" \
  -e GCS_TEST_BUCKET="${GCS_TEST_BUCKET}" \
  -e CI_NODE_TOTAL="${CI_NODE_TOTAL}" \
  -e CI_NODE_INDEX="${CI_NODE_INDEX}" \
  -e CI_ACCEPTANCE_TEST=true \
  -e TERM=dumb \
  $BUILD_IMAGE \ yarn && yarn test
