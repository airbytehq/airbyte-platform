# A test describe in the README is available to test a version update
ARG TEMPORAL_AUTO_SETUP_IMAGE=temporal/auto-setup:2023.5.19.1
FROM ${TEMPORAL_AUTO_SETUP_IMAGE}

ENV TEMPORAL_HOME /etc/temporal

COPY bin/scripts/update-and-start-temporal.sh update-and-start-temporal.sh

ENTRYPOINT ["./update-and-start-temporal.sh"]
