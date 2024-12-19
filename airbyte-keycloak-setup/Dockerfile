ARG JDK_IMAGE=airbyte/airbyte-base-java-image:3.3.1

FROM scratch as builder
WORKDIR /app
ADD airbyte-app.tar /app
ADD entrypoint.sh entrypoint.sh

FROM ${JDK_IMAGE} AS keycloak-setup
WORKDIR /app
COPY --chown=airbyte:airbyte --from=builder /app /app
RUN chmod +x /app/entrypoint.sh
USER airbyte:airbyte

ENTRYPOINT "/app/entrypoint.sh"
CMD ["/bin/bash", "-c", "airbyte-app/bin/airbyte-keycloak-setup"]
