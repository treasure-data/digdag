#!/bin/bash

# XML Reports
for dir in digdag-*/build/test-results; do
  project=$(dirname $(dirname ${dir}))
  mkdir -p "/tmp/circleci-artifacts/build/tests/${project}"
  cp -a "${dir}/" "/tmp/circleci-artifacts/build/tests/${project}/"
done

# HTML Reports
mkdir -p /tmp/circleci-artifacts/build/reports
for dir in build/reports digdag-*/build/reports; do
  mkdir -p /tmp/circleci-artifacts/build/reports/${dir%%/*}
  cp -a $dir /tmp/circleci-artifacts/build/reports/${dir%%/*}
done
