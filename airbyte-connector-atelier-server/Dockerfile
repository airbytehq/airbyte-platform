ARG JDK_IMAGE=airbyte/airbyte-base-java-image:2.0.2
FROM ${JDK_IMAGE} AS connector-atelier-server

RUN apk add --update \
    build-base \
    openssl-dev \
    bzip2-dev \
    libffi-dev \
    zlib-dev \
    sqlite-dev \
    xz-dev

# Set up CDK requirements
ARG PYTHON_VERSION=3.9.11
ARG CDK_VERSION=0.36.4
# Set up python
RUN git clone https://github.com/pyenv/pyenv.git ~/.pyenv
ENV PYENV_ROOT /root/.pyenv
ENV PATH ${PYENV_ROOT}/shims:${PYENV_ROOT}/bin:$PATH
RUN pyenv install ${PYTHON_VERSION} && pyenv global ${PYTHON_VERSION}
ENV CDK_PYTHON=${PYENV_ROOT}/versions/${PYTHON_VERSION}/bin/python
ENV CDK_ENTRYPOINT ${PYENV_ROOT}/versions/${PYTHON_VERSION}/lib/python3.9/site-packages/airbyte_cdk/connector_builder/main.py
# Set up CDK
ENV PIP=${PYENV_ROOT}/versions/${PYTHON_VERSION}/bin/pip
RUN ${PIP} install airbyte-cdk==${CDK_VERSION}

ARG VERSION=0.44.4

ENV APPLICATION airbyte-connector-atelier-server
ENV VERSION ${VERSION}

WORKDIR /app

# This is automatically unzipped by Docker
ADD bin/${APPLICATION}-${VERSION}.tar /app

# wait for upstream dependencies to become available before starting server
ENTRYPOINT ["/bin/bash", "-c", "${APPLICATION}-${VERSION}/bin/${APPLICATION}"]

LABEL io.airbyte.version=0.44.4
LABEL io.airbyte.name=airbyte/connector-atelier-server
