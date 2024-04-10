ARG JDK_IMAGE=airbyte/airbyte-base-java-image:3.2.0
FROM ${JDK_IMAGE}

WORKDIR /app

USER root
ADD airbyte-app.tar /app
RUN chown -R airbyte:airbyte /app
USER airbyte:airbyte

ENTRYPOINT ["/bin/bash", "-c", "airbyte-app/bin/airbyte-cron"]
