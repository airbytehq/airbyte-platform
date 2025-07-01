FROM postgres:13-alpine

COPY --chmod=755 bin/init.sql /docker-entrypoint-initdb.d/000_init.sql
COPY --chmod=755 bin/airbyte-entrypoint.sh /usr/local/bin/

ENTRYPOINT ["airbyte-entrypoint.sh"]

