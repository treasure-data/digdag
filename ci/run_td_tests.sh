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
export REDIS_HOST=127.0.0.1

export CI_ACCEPTANCE_TEST=true

./gradlew clean cleanTest test --info --no-daemon -p digdag-tests --tests 'acceptance.td.*'
