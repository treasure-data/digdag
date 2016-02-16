#!/usr/bin/env bash

if [ ! -f .digdag-wrapper/digdag.jar ]; then
    echo "Downloading the latest digdag package..."
    rm -f .digdag-wrapper/digdag.jar.downloading
    curl --create-dirs -o .digdag-wrapper/digdag.jar.downloading -L "https://dl.digdag.io/digdag-latest.jar" `cat .digdag-wrapper/download-options`
    if [ ! -f .digdag-wrapper/digdag.jar.downloading ]; then
        echo "Download failed. You may need .digdag-wrapper/download-options file."
        exit 1
    fi
    chmod +x .digdag-wrapper/digdag.jar.downloading
    mv -f .digdag-wrapper/digdag.jar.downloading .digdag-wrapper/digdag.jar
fi

exec .digdag-wrapper/digdag.jar "$@"
