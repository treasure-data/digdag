#!/bin/bash

# XML Reports
for dir in digdag-*/build/test-results; do
  project=$(dirname $(dirname ${dir}))
  mkdir -p "${CIRCLE_TEST_REPORTS}/${project}"
  cp -a "${dir}/" "$CIRCLE_TEST_REPORTS/${project}/"
done

# HTML Reports
mkdir -p $CIRCLE_ARTIFACTS/reports
for dir in build/reports digdag-*/build/reports; do
  mkdir -p $CIRCLE_ARTIFACTS/reports/${dir%%/*}
  cp -a $dir $CIRCLE_ARTIFACTS/reports/${dir%%/*}
done
