#!/bin/bash -xe
docker run \
-w /digdag \
-v `pwd`/:/digdag \
-v ~/.gradle:/root/.gradle \
-e GITHUB_TOKEN \
digdag-build \
./circle/push_gh_pages.sh
