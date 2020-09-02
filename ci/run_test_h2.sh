#!/bin/bash -xe
BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

$BASEDIR/run_test_docker.sh "$@"
