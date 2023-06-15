ARG JDK_IMAGE=airbyte/airbyte-base-java-image:2.0.2
FROM ${JDK_IMAGE} AS worker

ARG DOCKER_BUILD_ARCH=amd64

RUN apk add jq tar

RUN wget "https://dl.k8s.io/release/$(wget -qO- https://dl.k8s.io/release/stable.txt)/bin/linux/${DOCKER_BUILD_ARCH}/kubectl" \
        && chmod +x kubectl && mv kubectl /usr/local/bin/

# Don't change this manually.  Bump version expects to make moves based on this string
ARG VERSION=0.44.4

ENV APPLICATION airbyte-workers
ENV VERSION ${VERSION}

WORKDIR /app

COPY WellKnownTypes.json /app

# Move worker app
ADD bin/${APPLICATION}-${VERSION}.tar /app

# Grab well-known types file
COPY WellKnownTypes.json /app

# wait for upstream dependencies to become available before starting server
ENTRYPOINT ["/bin/sh", "-c", "${APPLICATION}-${VERSION}/bin/${APPLICATION}"]
