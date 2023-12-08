ARG JDK_IMAGE=airbyte/airbyte-base-java-image:2.0.4
FROM ${JDK_IMAGE}
WORKDIR /app
ADD airbyte-app.tar /app
ENTRYPOINT ["/bin/bash", "-c", "airbyte-app/bin/airbyte-metrics-reporter"]
