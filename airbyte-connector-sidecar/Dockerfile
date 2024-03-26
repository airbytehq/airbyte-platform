ARG JAVA_WORKER_BASE_IMAGE_VERSION=2.1.0
FROM airbyte/airbyte-base-java-worker-image:${JAVA_WORKER_BASE_IMAGE_VERSION}

ARG DOCKER_BUILD_ARCH=amd64

# Don't change this manually.  Bump version expects to make moves based on this string
ARG VERSION=dev

ENV APPLICATION airbyte-connector-sidecar
ENV VERSION=${VERSION}

USER root
COPY WellKnownTypes.json /app

# Move connector-sidecar app
ADD airbyte-app.tar /app
RUN chown -R airbyte:airbyte /app
USER airbyte:airbyte

# wait for upstream dependencies to become available before starting server
ENTRYPOINT ["/bin/bash", "-c", "/app/airbyte-app/bin/${APPLICATION}"]
