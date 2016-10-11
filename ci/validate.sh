#!/bin/bash -xe
docker run \
-w /digdag \
-v `pwd`/:/digdag \
-v ~/.gradle:/root/.gradle \
-v ~/.m2:/root/.m2 \
-i \
danieln/digdag-build:20160823085939Z-4c3c94d6925e4e476fca04260ec6be6352351de2 \
bash -ex <<EOF

# Verify that gradle can generate a working maven pom and generate a dependency tree
./gradlew clean pom --info --no-daemon
mvn compile test-compile dependency:tree

# Run the maven enforcer
mvn validate

EOF
