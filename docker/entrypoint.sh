#!/bin/bash -e

if [ ! -z "$DIGDAG_TEST_POSTGRESQL" ]; then
    /etc/init.d/postgresql start
fi

if [ ! -z "$DIGDAG_TEST_REDIS" ]; then
    redis-server &
fi

export TEST_S3_ACCESS_KEY_ID=$(cat /dev/urandom | LC_CTYPE=C tr -dc 'a-zA-Z0-9' | fold -w 16 | head -n 1)
export TEST_S3_SECRET_ACCESS_KEY=$(cat /dev/urandom | LC_CTYPE=C tr -dc 'a-zA-Z0-9' | fold -w 16 | head -n 1)
export TEST_S3_ENDPOINT=http://127.0.0.1:9000

export MINIO_ACCESS_KEY=${TEST_S3_ACCESS_KEY_ID}
export MINIO_SECRET_KEY=${TEST_S3_SECRET_ACCESS_KEY}

mkdir -p /tmp/minio
minio server /tmp/minio/ &

exec "$@"
