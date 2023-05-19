# A test describe in the README is available to test a version update
ARG TEMPORAL_AUTO_SETUP_IMAGE=airbyte/temporal-auto-setup:1.20.3
FROM ${TEMPORAL_AUTO_SETUP_IMAGE}

ENV TEMPORAL_HOME /etc/temporal

COPY bin/scripts/update-and-start-temporal.sh update-and-start-temporal.sh

ENTRYPOINT ["./update-and-start-temporal.sh"]
