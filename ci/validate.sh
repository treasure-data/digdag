#!/bin/bash -xe
docker run \
-w /digdag \
-v `pwd`/:/digdag \
-v ~/.gradle:/root/.gradle \
-v ~/.m2:/root/.m2 \
-i \
$BUILD_IMAGE \
bash -ex <<EOF

# Verify that gradle can generate a working maven pom and generate a dependency tree
./gradlew clean pom --info --no-daemon
mvn compile test-compile dependency:tree

# Run the maven enforcer
mvn validate

EOF
