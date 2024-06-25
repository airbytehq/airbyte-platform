ARG JDK_IMAGE=airbyte/airbyte-base-java-image:3.2.3

FROM ${JDK_IMAGE}
WORKDIR /app
ADD --chown=airbyte:airbyte airbyte-app.tar /app
USER airbyte:airbyte

ENTRYPOINT ["/bin/bash", "-c", "airbyte-app/bin/airbyte-cron"]
