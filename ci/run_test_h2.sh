#!/bin/bash -xe
BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ ${CIRCLECI} ]; then
    $BASEDIR/run_test_simple.sh
else
    $BASEDIR/run_test_docker.sh
fi
