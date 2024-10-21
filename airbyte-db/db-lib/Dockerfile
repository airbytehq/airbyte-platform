FROM postgres:13-alpine

COPY bin/init.sql /docker-entrypoint-initdb.d/000_init.sql
COPY bin/airbyte-entrypoint.sh /usr/local/bin/

ENTRYPOINT ["airbyte-entrypoint.sh"]

