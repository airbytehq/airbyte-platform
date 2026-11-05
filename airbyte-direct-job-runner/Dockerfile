ARG JAVA_BASE_IMAGE_VERSION=3.3
ARG SOURCE_POSTGRES_VERSION=3.8.10

# The connector runs as a local process (see DirectCheckRunner), so its distribution ships inside this image.
FROM airbyte/source-postgres:${SOURCE_POSTGRES_VERSION} AS source-postgres

FROM scratch as builder
WORKDIR /app
ADD airbyte-app.tar /app

FROM airbyte/airbyte-base-java-image:${JAVA_BASE_IMAGE_VERSION}

# 8080 is the port micronaut listens on
# 8085 is the Micronaut management endpoint port
# 5005 is the remote debug port
EXPOSE 8080 8085 5005

ARG VERSION=dev
ARG SOURCE_POSTGRES_VERSION

ENV APPLICATION airbyte-direct-job-runner
ENV VERSION=${VERSION}
# The runner only takes checks for this exact image; the launcher sends every other version to a pod.
ENV CHECK_CONNECTOR_IMAGE=airbyte/source-postgres:${SOURCE_POSTGRES_VERSION}

WORKDIR /app
COPY --chown=airbyte:airbyte --from=builder /app /app
COPY --chown=airbyte:airbyte --from=source-postgres /airbyte /connectors/source-postgres
USER airbyte:airbyte

CMD ["/bin/bash", "-c", "/app/airbyte-app/bin/${APPLICATION}"]
