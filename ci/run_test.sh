#!/bin/bash -e
BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ 0 -eq ${CI_NODE_INDEX} ]; then
    echo "run_findbugs.sh and others"
    "${BASEDIR}/run_findbugs.sh"
    "${BASEDIR}/validate.sh"
    "${BASEDIR}/run_build.sh"
fi

if [ 0 -lt ${CI_NODE_INDEX} -o 1 -eq ${CI_NODE_TOTAL} ]; then
    echo "run_test_(h2|pg).sh"
    ${BASEDIR}/run_test_h2.sh -PwithoutUi
    ${BASEDIR}/run_test_pg.sh -PwithoutUi
fi



