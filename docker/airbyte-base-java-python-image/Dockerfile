ARG JDK_IMAGE=airbyte/airbyte-base-java-image:3.2.1
FROM ${JDK_IMAGE}

USER root

RUN yum update -y && \
    yum groupinstall -y "Development Tools" && \
    yum install -y gcc make patch zlib-devel bzip2 bzip2-devel readline-devel sqlite sqlite-devel openssl11-devel tk-devel libffi-devel xz-devel

USER airbyte:airbyte

ENV PYTHON_VERSION=3.9.11

# Set up python
RUN git clone https://github.com/pyenv/pyenv.git ~/.pyenv
ENV PYENV_ROOT /home/airbyte/.pyenv
ENV PATH ${PYENV_ROOT}/shims:${PYENV_ROOT}/bin:$PATH
RUN pyenv install -v ${PYTHON_VERSION} && pyenv global ${PYTHON_VERSION}
