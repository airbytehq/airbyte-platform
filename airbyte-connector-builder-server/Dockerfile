ARG BASE_IMAGE=airbyte/airbyte-base-java-python-image:1.1.0
FROM ${BASE_IMAGE} AS connector-builder-server

# Set up CDK requirements
ARG CDK_VERSION=0.57.0
ENV CDK_PYTHON=${PYENV_ROOT}/versions/${PYTHON_VERSION}/bin/python
ENV CDK_ENTRYPOINT ${PYENV_ROOT}/versions/${PYTHON_VERSION}/lib/python3.9/site-packages/airbyte_cdk/connector_builder/main.py
# Set up CDK
ENV PIP=${PYENV_ROOT}/versions/${PYTHON_VERSION}/bin/pip
COPY requirements.txt requirements.txt
RUN ${PIP} install -r requirements.txt

ARG VERSION=dev

ENV APPLICATION airbyte-connector-builder-server
ENV VERSION ${VERSION}

WORKDIR /app

# This is automatically unzipped by Docker
ADD airbyte-app.tar /app

# wait for upstream dependencies to become available before starting server
ENTRYPOINT ["/bin/bash", "-c", "airbyte-app/bin/${APPLICATION}"]

LABEL io.airbyte.version=${VERSION}
LABEL io.airbyte.name=airbyte/connector-builder-server
