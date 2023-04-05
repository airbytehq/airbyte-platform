ARG JDK_IMAGE=airbyte/airbyte-base-java-image:2.0.2
FROM ${JDK_IMAGE} AS server

EXPOSE 8000

ARG VERSION=0.42.1

ENV APPLICATION airbyte-server
ENV VERSION ${VERSION}

WORKDIR /app

# This is automatically unzipped by Docker
ADD bin/${APPLICATION}-${VERSION}.tar /app

# wait for upstream dependencies to become available before starting server
ENTRYPOINT ["/bin/bash", "-c", "${APPLICATION}-${VERSION}/bin/${APPLICATION}"]
