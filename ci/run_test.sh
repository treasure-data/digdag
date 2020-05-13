#!/bin/bash -e
BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [[ $(( 0 % ${CI_NODE_TOTAL} )) -eq ${CI_NODE_INDEX} ]]; then
    "${BASEDIR}/run_findbugs.sh"
fi

if [[ $(( 1 % ${CI_NODE_TOTAL} )) -eq ${CI_NODE_INDEX} ]]; then
    "${BASEDIR}/validate.sh"
fi

if [[ $(( 2 % ${CI_NODE_TOTAL} )) -eq ${CI_NODE_INDEX} ]]; then
  UI_BUILD_OPT=""
else
  # Don't need to build UI always
  UI_BUILD_OPT="-PwithoutUi"
fi

"${BASEDIR}/run_test_h2.sh"
"${BASEDIR}/run_test_pg.sh ${UI_BUILD_OPT}"

