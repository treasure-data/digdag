#!/bin/bash -e
BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# All SpotBugs reports are stored in spotbugs-all/
mkdir spotbugs-all
if [[ $(( 0 % ${CI_NODE_TOTAL} )) -eq ${CI_NODE_INDEX} ]]; then
    "${BASEDIR}/run_findbugs.sh"

    mkdir spotbugs-all/"${CI_NODE_INDEX}"
    for s in digdag-*/build/reports/spotbugs
    do
      subpj=$(echo "$s" |sed -E 's/\/build\/reports\/spotbugs//')
      echo "$subpj"
      dstdir=spotbugs-all/"${CI_NODE_INDEX}"/"${subpj}"
      mkdir "$dstdir"
      cp "${s}"/*html "${dstdir}"/
    done
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

mkdir jacoco-all
# It's enough to build UI either with H2 or PostgreSQL not with the both
"${BASEDIR}/run_test_h2.sh" ${WITHOUT_BUILD_UI}
cp digdag-*/build/jacoco/*.exec jacoco-all/
"${BASEDIR}/run_test_pg.sh" ${BUILD_UI_OPT}
cp digdag-*/build/jacoco/*.exec jacoco-all/
