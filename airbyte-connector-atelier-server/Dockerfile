ARG JDK_IMAGE=airbyte/airbyte-base-java-image:1.0
FROM ${JDK_IMAGE} AS connector-atelier-server

ARG VERSION=0.42.0

ENV APPLICATION airbyte-connector-atelier-server
ENV VERSION ${VERSION}

WORKDIR /app

# This is automatically unzipped by Docker
ADD bin/${APPLICATION}-${VERSION}.tar /app

# wait for upstream dependencies to become available before starting server
ENTRYPOINT ["/bin/bash", "-c", "${APPLICATION}-${VERSION}/bin/${APPLICATION}"]

LABEL io.airbyte.version=0.42.0
LABEL io.airbyte.name=airbyte/connector-atelier-server