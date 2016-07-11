#!/bin/bash -xe

docker run \
-w /digdag \
-v `pwd`/:/digdag \
-v ~/.gradle:/root/.gradle \
-e TD_API_KEY="${TD_API_KEY}" \
-e CI_NODE_TOTAL="${CI_NODE_TOTAL}" \
-e CI_NODE_INDEX="${CI_NODE_INDEX}" \
-e CI_ACCEPTANCE_TEST=true \
digdag-build \
./gradlew clean test --info --no-daemon
