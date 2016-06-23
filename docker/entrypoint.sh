#!/bin/bash -e
/etc/init.d/postgresql start

fakes3 -r /tmp/fakes3 -p 4567 &
export FAKE_S3_ENDPOINT=http://127.0.0.1:4567

exec "$@"
