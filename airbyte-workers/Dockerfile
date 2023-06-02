ARG JDK_IMAGE=airbyte/airbyte-base-java-image:2.0.3
FROM ${JDK_IMAGE} AS worker

ARG DOCKER_BUILD_ARCH=amd64

RUN amazon-linux-extras install -y docker
RUN yum install -y jq tar && yum clean all

RUN curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/${DOCKER_BUILD_ARCH}/kubectl" \
        && chmod +x kubectl && mv kubectl /usr/local/bin/

# Don't change this manually.  Bump version expects to make moves based on this string
ARG VERSION=0.44.8

ENV APPLICATION airbyte-workers
ENV VERSION ${VERSION}

WORKDIR /app

COPY WellKnownTypes.json /app

# Move worker app
ADD bin/${APPLICATION}-${VERSION}.tar /app

# Grab well-known types file
COPY WellKnownTypes.json /app

EXPOSE 5007

# wait for upstream dependencies to become available before starting server
ENTRYPOINT ["/bin/bash", "-c", "${APPLICATION}-${VERSION}/bin/${APPLICATION}"]
