#!/usr/bin/env bash

if [[ ! -f .digdag-wrapper/digdag.jar ]]; then
    echo "Downloading digdag wrapper is not implemented yet."
    echo "Please put .digdag-wrapper.jar file and set executable permission to it."
    exit 1
fi

exec .digdag-wrapper/digdag.jar "$@"
