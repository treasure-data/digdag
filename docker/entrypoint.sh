#!/bin/bash -e
/etc/init.d/postgresql start
exec "$@"
