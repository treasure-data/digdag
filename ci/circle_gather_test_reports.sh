#!/bin/bash -e
for dir in digdag-*/build/test-results; do
  project=$(dirname $(dirname ${dir}))
  mkdir -p "${CIRCLE_TEST_REPORTS}/${project}"
  cp -a "${dir}/" "$CIRCLE_TEST_REPORTS/${project}/"
done
