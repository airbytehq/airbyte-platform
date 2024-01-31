FROM airbyte/airbyte-base-java-image:2.1.0

ARG TARGETPLATFORM

RUN amazon-linux-extras install -y docker
RUN yum install -y jq tar && yum clean all

RUN curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/$TARGETPLATFORM/kubectl" \
        && chmod +x kubectl && mv kubectl /usr/local/bin/
