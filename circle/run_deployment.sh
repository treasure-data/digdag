#!/bin/bash -xe
docker run \
-w /digdag \
-v `pwd`/:/digdag \
-v ~/.gradle:/root/.gradle \
digdag-build \
./circle/push_gh_pages.sh
