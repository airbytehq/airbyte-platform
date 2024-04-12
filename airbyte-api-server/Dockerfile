ARG JDK_IMAGE=airbyte/airbyte-base-java-image:3.2.1

FROM scratch as builder
WORKDIR /app
ADD airbyte-app.tar /app

FROM ${JDK_IMAGE}
EXPOSE 8006 5005
ENV APPLICATION airbyte-api-server
ENV VERSION ${VERSION}
WORKDIR /app
COPY --chown=airbyte:airbyte --from=builder /app /app
USER airbyte:airbyte

# wait for upstream dependencies to become available before starting server
ENTRYPOINT ["/bin/bash", "-c", "airbyte-app/bin/${APPLICATION}"]
