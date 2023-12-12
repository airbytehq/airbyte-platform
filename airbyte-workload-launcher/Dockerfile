ARG JDK_IMAGE=airbyte/airbyte-base-java-image:2.1.0
FROM ${JDK_IMAGE} AS airbyte-workload-launcher

# Seems to also be here to enable kubectl cmds (namely, cp)
ARG DOCKER_BUILD_ARCH=amd64

RUN amazon-linux-extras install -y docker

# We need this for kubectl cmds which are just wrappers around native cmds
RUN yum install -y jq tar && yum clean all

# The launcher code runs kubectl cmds directly so we need this
RUN curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/${DOCKER_BUILD_ARCH}/kubectl" \
        && chmod +x kubectl && mv kubectl /usr/local/bin/

ENV APPLICATION airbyte-workload-launcher
ENV VERSION ${VERSION}

WORKDIR /app

# This is automatically unzipped by Docker
ADD airbyte-app.tar /app

# 8016 is the port micronaut listens on
# 5005 is the remote debug port
EXPOSE 8016 5005

# wait for upstream dependencies to become available before starting server
ENTRYPOINT ["/bin/bash", "-c", "airbyte-app/bin/${APPLICATION}"]
