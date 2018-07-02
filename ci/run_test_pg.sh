#!/bin/bash -xe
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

if [ ${CIRCLECI} ]; then
    $BASEDIR/run_test_simple.sh
else
    $BASEDIR/run_test_docker.sh
fi
