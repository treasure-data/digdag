Docker Build Environment
========================

This directory contains a Makefile and a Dockerfile to create a docker image for use as a build environment for digdag.

The docker image is used by the CI environments.

When making build image changes, remember to modify CI environment configuration files to make use of the new image.


Building Image
--------------

	make

Pushing Image
-------------

	make push
