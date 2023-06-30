ARG JDK_IMAGE=airbyte/airbyte-base-java-image:2.0.3
FROM ${JDK_IMAGE} AS server

EXPOSE 8000 5005

ARG VERSION=0.50.5

ENV APPLICATION airbyte-server
ENV VERSION ${VERSION}

RUN addgroup -S appgroup && \
    adduser -S appuser -G appgroup -u 1000
USER appuser

WORKDIR /app

# This is automatically unzipped by Docker
ADD bin/${APPLICATION}-${VERSION}.tar /app

# wait for upstream dependencies to become available before starting server
ENTRYPOINT ["/bin/sh", "-c", "${APPLICATION}-${VERSION}/bin/${APPLICATION}"]
