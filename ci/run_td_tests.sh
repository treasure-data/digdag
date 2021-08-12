#!/bin/bash -e
BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export DIGDAG_TEST_POSTGRESQL="
host = localhost
port = 5432
user = digdag_test
password =
database = digdag_test
idleTimeout = 10
minimumPoolSize = 0
"

echo "---TARGET test ---"
target_src1=`circleci tests glob "digdag-tests/src/test/java/acceptance/**/*IT.java" | circleci tests split --split-by=timings`
# Exclude some tests due to failure in CircleCI. Will fix them later.
target_src=`echo $target_src1 | sed -E 's/ digdag-tests\/src\/test\/java\/acceptance\/(S3Wait|S3Storage|Docker)IT.java//g'`
echo $target_src | xargs -n 1 echo
echo "------------------"


export CI_ACCEPTANCE_TEST=true

./gradlew clean cleanTest test --info --no-daemon -p digdag-tests --tests 'acceptance.*' \
	  -PtestFilter="$target_src"


