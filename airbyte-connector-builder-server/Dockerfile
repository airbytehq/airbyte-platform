ARG JAVA_PYTHON_BASE_IMAGE_VERSION=2.2.5
FROM airbyte/airbyte-base-java-python-image:${JAVA_PYTHON_BASE_IMAGE_VERSION} AS connector-builder-server

# Set up CDK requirements
ARG CDK_VERSION=6.48.15
ENV CDK_PYTHON=${PYENV_ROOT}/versions/${PYTHON_VERSION}/bin/python
ENV CDK_ENTRYPOINT ${PYENV_ROOT}/versions/${PYTHON_VERSION}/lib/python3.10/site-packages/airbyte_cdk/connector_builder/main.py
# Set up CDK
ENV PIP=${PYENV_ROOT}/versions/${PYTHON_VERSION}/bin/pip
COPY --chown=airbyte:airbyte requirements.txt requirements.txt
RUN ${PIP} install -r requirements.txt

ARG VERSION=dev

ENV APPLICATION airbyte-connector-builder-server
ENV VERSION ${VERSION}

WORKDIR /app

# This is automatically unzipped by Docker
USER root
ADD airbyte-app.tar /app
RUN chown -R airbyte:airbyte /app
USER airbyte:airbyte

# wait for upstream dependencies to become available before starting server
CMD ["/bin/bash", "-c", "airbyte-app/bin/${APPLICATION}"]

LABEL io.airbyte.version=${VERSION}
LABEL io.airbyte.name=airbyte/connector-builder-server
