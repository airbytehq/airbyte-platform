ARG JDK_IMAGE=airbyte/airbyte-base-java-image:2.0.2
FROM ${JDK_IMAGE} AS connector-atelier-server

RUN apk add --update \
    build-base \
    openssl-dev \
    bzip2-dev \
    libffi-dev \
    zlib-dev \
    sqlite-dev \
    xz-dev \
    git \
    bash

# Set up CDK requirements
ARG CDK_VERSION=0.44.0
ENV CDK_PYTHON=${PYENV_ROOT}/versions/${PYTHON_VERSION}/bin/python
ENV CDK_ENTRYPOINT ${PYENV_ROOT}/versions/${PYTHON_VERSION}/lib/python3.9/site-packages/airbyte_cdk/connector_builder/main.py
# Set up CDK
ENV PIP=${PYENV_ROOT}/versions/${PYTHON_VERSION}/bin/pip
COPY requirements.txt requirements.txt
RUN ${PIP} install -r requirements.txt

ARG VERSION=0.50.5

ENV APPLICATION airbyte-connector-atelier-server
ENV VERSION ${VERSION}

WORKDIR /app

# This is automatically unzipped by Docker
ADD bin/${APPLICATION}-${VERSION}.tar /app

# wait for upstream dependencies to become available before starting server
ENTRYPOINT ["/bin/bash", "-c", "${APPLICATION}-${VERSION}/bin/${APPLICATION}"]

LABEL io.airbyte.version=0.50.5
LABEL io.airbyte.name=airbyte/connector-atelier-server
