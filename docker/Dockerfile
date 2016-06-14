FROM ubuntu:14.04

ENV DEBIAN_FRONTEND noninteractive
ENV TERM dumb

WORKDIR /bootstrap
COPY bootstrap /bootstrap
RUN ./dependencies.sh
