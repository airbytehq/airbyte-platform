ARG JDK_IMAGE=airbyte/airbyte-base-java-image:3.0.1
FROM ${JDK_IMAGE} AS keycloak-setup
WORKDIR /app
ADD airbyte-app.tar /app
ENTRYPOINT ["/bin/bash", "-c", "airbyte-app/bin/airbyte-keycloak-setup"]
