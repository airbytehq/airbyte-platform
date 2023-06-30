ARG JDK_IMAGE=airbyte/airbyte-base-java-image:2.0.3
FROM ${JDK_IMAGE} AS orchestrator

ARG DOCKER_BUILD_ARCH=amd64

RUN apk add docker jq tar

RUN wget "https://dl.k8s.io/release/$(wget -qO-  https://dl.k8s.io/release/stable.txt)/bin/linux/${DOCKER_BUILD_ARCH}/kubectl" \
        && chmod +x kubectl && mv kubectl /usr/local/bin/

# Don't change this manually.  Bump version expects to make moves based on this string
ARG VERSION=0.50.5

ENV APPLICATION airbyte-container-orchestrator
ENV VERSION=${VERSION}
ENV AIRBYTE_ENTRYPOINT "/app/${APPLICATION}-${VERSION}/bin/${APPLICATION}"

WORKDIR /app

COPY WellKnownTypes.json /app

# Move orchestrator app
ADD bin/${APPLICATION}-${VERSION}.tar /app

# wait for upstream dependencies to become available before starting server
ENTRYPOINT ["/bin/bash", "-c", "/app/${APPLICATION}-${VERSION}/bin/${APPLICATION}"]
