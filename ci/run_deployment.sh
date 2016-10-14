#!/bin/bash -xe
docker run \
-w /digdag \
-v `pwd`/:/digdag \
-v ~/.gradle:/root/.gradle \
-e GITHUB_TOKEN \
$BUILD_IMAGE \
ci/push_gh_pages.sh
