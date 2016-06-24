#!/bin/bash -xe
docker run \
-w /digdag \
-v `pwd`/:/digdag \
-v ~/.gradle:/root/.gradle \
-v ~/.m2:/root/.m2 \
-i \
digdag-build \
bash -ex <<EOF

# Verify that gradle can generate a working maven pom
./gradlew clean pom --info --no-daemon
mvn compile test-compile

EOF
