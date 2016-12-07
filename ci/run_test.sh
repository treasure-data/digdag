#!/bin/bash -e
BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [[ $(( 0 % ${CI_NODE_TOTAL} )) -eq ${CI_NODE_INDEX} ]]; then
    "${BASEDIR}/run_findbugs.sh"
fi

if [[ $(( 1 % ${CI_NODE_TOTAL} )) -eq ${CI_NODE_INDEX} ]]; then
    "${BASEDIR}/validate.sh"
fi

"${BASEDIR}/run_test_h2.sh"
"${BASEDIR}/run_test_pg.sh"
"${BASEDIR}/run_test_ui.sh"
