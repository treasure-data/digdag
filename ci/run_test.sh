#!/bin/bash
BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export MIN_CI_NODES=4

: "${CI_NODE_INDEX:?CI_NODE_INDEX required}"

if [[ ${CI_NODE_TOTAL} -lt ${MIN_CI_NODES} ]]; then
  echo "Not enough CI nodes: got ${CI_NODE_TOTAL}, need ${MIN_CI_NODES} "
  exit 1
fi

case "${CI_NODE_INDEX}" in
0)
  "${BASEDIR}/run_test_h2.sh"
  ;;
1)
  "${BASEDIR}/run_test_pg.sh"
  ;;
2)
  "${BASEDIR}/run_findbugs.sh"
  ;;
3)
  "${BASEDIR}/validate.sh"
  ;;
*)
  echo "Extraneous CI node: ${CI_NODE_INDEX}"
esac
