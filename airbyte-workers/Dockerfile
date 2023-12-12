ARG JDK_IMAGE=airbyte/airbyte-base-java-image:2.1.0
FROM ${JDK_IMAGE} AS worker

ARG DOCKER_BUILD_ARCH=amd64

RUN amazon-linux-extras install -y docker
RUN yum install -y jq tar && yum clean all

RUN curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/${DOCKER_BUILD_ARCH}/kubectl" \
        && chmod +x kubectl && mv kubectl /usr/local/bin/

ENV APPLICATION airbyte-workers
ENV VERSION ${VERSION}

WORKDIR /app

COPY WellKnownTypes.json /app

# Move worker app
ADD airbyte-app.tar /app

EXPOSE 5005

# wait for upstream dependencies to become available before starting server
ENTRYPOINT ["/bin/bash", "-c", "airbyte-app/bin/${APPLICATION}"]
