FROM amazoncorretto:21 AS connector-sidecar

ARG DOCKER_BUILD_ARCH=amd64

# Don't change this manually.  Bump version expects to make moves based on this string
ARG VERSION=dev

ENV APPLICATION airbyte-connector-sidecar
ENV VERSION=${VERSION}

WORKDIR /app

COPY WellKnownTypes.json /app

# Move connector-sidecar app
ADD airbyte-app.tar /app

# wait for upstream dependencies to become available before starting server
ENTRYPOINT ["/bin/bash", "-c", "/app/airbyte-app/bin/${APPLICATION}"]
