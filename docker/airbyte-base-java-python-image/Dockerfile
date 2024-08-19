ARG JDK_IMAGE=airbyte/airbyte-base-java-image:3.2.5
FROM ${JDK_IMAGE}

USER root

RUN apk update && \
    apk upgrade && \
    apk add --no-cache git build-base gcc make patch zlib-dev bzip2 bzip2-dev readline-dev sqlite sqlite-dev openssl-dev tk-dev libffi-dev xz-dev

USER airbyte:airbyte

ENV PYTHON_VERSION=3.10.14

# Set up python
RUN git clone https://github.com/pyenv/pyenv.git ~/.pyenv
ENV PYENV_ROOT /home/airbyte/.pyenv
ENV PATH ${PYENV_ROOT}/shims:${PYENV_ROOT}/bin:$PATH
RUN pyenv install -v ${PYTHON_VERSION} && pyenv global ${PYTHON_VERSION}
RUN python -m pip install --upgrade pip setuptools
