#!/bin/bash
BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

: "${CI_NODE_INDEX:?CI_NODE_INDEX required}"

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
*)
  echo "Invalid CI_NODE_INDEX: ${CI_NODE_INDEX}"
  exit 1
esac
