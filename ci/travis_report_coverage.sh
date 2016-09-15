#!/bin/bash -xe

# TRAVIS_* and CI_NAME environment variables are required by
# org.kt3k.gradle.plugin.coveralls.domain.ServiceInfoFactory.

docker run \
-w /digdag \
-v `pwd`/:/digdag \
-v ~/.gradle:/root/.gradle \
-e TERM=dumb \
-e TRAVIS \
-e TRAVIS_JOB_ID \
-e TRAVIS_JOB_ID \
-e TRAVIS_PULL_REQUEST \
-e CI_NAME \
digdag-build \
./gradlew jacocoTestReport coveralls --info --no-daemon "$@"
