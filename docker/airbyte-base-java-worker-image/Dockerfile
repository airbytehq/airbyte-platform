ARG JDK_IMAGE=airbyte/airbyte-base-java-image:3.2.0
FROM ${JDK_IMAGE}

ARG TARGETPLATFORM

USER root

RUN amazon-linux-extras install -y docker
RUN yum install -y jq tar && yum clean all

RUN curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/$TARGETPLATFORM/kubectl" \
        && chmod +x kubectl && mv kubectl /usr/local/bin/

RUN mkdir -p /tmp/workspace && chown -R airbyte:airbyte /tmp/workspace
RUN mkdir -p /tmp/airbyte_local && chown -R airbyte:airbyte /tmp/airbyte_local

USER airbyte:airbyte
