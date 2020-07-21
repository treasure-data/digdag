#!/bin/bash -e
BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [[ $(( 0 % ${CI_NODE_TOTAL} )) -eq ${CI_NODE_INDEX} ]]; then
    "${BASEDIR}/run_findbugs.sh"
fi

if [[ $(( 1 % ${CI_NODE_TOTAL} )) -eq ${CI_NODE_INDEX} ]]; then
    "${BASEDIR}/validate.sh"
fi

WITH_BUILD_UI=""
WITHOUT_BUILD_UI="-x buildUi"

# Don't need to build UI always
if [[ $(( 2 % ${CI_NODE_TOTAL} )) -eq ${CI_NODE_INDEX} ]]; then
  BUILD_UI_OPT=${WITH_BUILD_UI}
else
  BUILD_UI_OPT=${WITHOUT_BUILD_UI}
fi

# It's enough to build UI either with H2 or PostgreSQL not with the both
"${BASEDIR}/run_test_h2.sh" ${WITHOUT_BUILD_UI}
"${BASEDIR}/run_test_pg.sh" ${BUILD_UI_OPT}

