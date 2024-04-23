ARG JDK_IMAGE=airbyte/airbyte-base-java-image:3.2.1

FROM scratch as builder
WORKDIR /app
ADD airbyte-app.tar /app

FROM ${JDK_IMAGE} AS server
EXPOSE 8000 5005
ARG VERSION=0.57.4
ENV APPLICATION airbyte-server
ENV VERSION ${VERSION}
WORKDIR /app
COPY --chown=airbyte:airbyte --from=builder /app /app
USER airbyte:airbyte

# wait for upstream dependencies to become available before starting server
ENTRYPOINT ["/bin/bash", "-c", "airbyte-app/bin/${APPLICATION}"]
