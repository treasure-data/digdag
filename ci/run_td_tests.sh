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
target_src=`circleci tests glob "digdag-tests/src/test/java/acceptance/td/**/*IT.java" | circleci tests split --split-by=timings`
echo $target_src | xargs -n 1 echo
echo "------------------"


export CI_ACCEPTANCE_TEST=true

./gradlew clean cleanTest test --info --no-daemon -p digdag-tests --tests 'acceptance.td.*' \
	  -PtestFilter="$target_src"
#	  -PtestFilter='`circleci tests glob "digdag-tests/src/test/java/acceptance/td/**/*.java" | circleci tests split --split-by=timings`'


