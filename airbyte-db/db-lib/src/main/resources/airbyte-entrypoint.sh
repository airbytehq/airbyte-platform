#!/bin/sh

# ensure that the pgdata dir exists and has the proper owner.
mkdir -p $PGDATA
chown 70:70 $PGDATA

docker-entrypoint.sh postgres
