ARG JDK_IMAGE=airbyte/airbyte-base-java-image:2.1.0
FROM ${JDK_IMAGE} AS keycloak-setup
WORKDIR /app
ADD airbyte-app.tar /app
ENTRYPOINT ["/bin/bash", "-c", "airbyte-app/bin/airbyte-keycloak-setup"]
