Docker Build Environment
========================

This directory contains scripts and a Dockerfile to create a docker image for use as a build environment for digdag.

* `setup.sh`
Loads the cached docker image from `~/docker/build.tar`, if available and then invokes `docker build`, to either (re)build or update the docker image if necessary. The docker image is then saved to `~/docker/build.tar`.

* `entrypoint.sh`
The `ENTRYPOINT` of the docker image. Starts postgresql.

* `bootstrap/`
Configuration files and scripts that are injected into the docker image when building it to install digdag build dependencies.

* `Dockerfile`
Invoked by `setup.sh` to create the docker image.