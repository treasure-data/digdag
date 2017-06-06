#!/bin/bash -e
BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

"${BASEDIR}"/run_test_pg.sh -p digdag-tests --tests 'acceptance.td.*'

