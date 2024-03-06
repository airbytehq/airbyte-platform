FROM airbyte/airbyte-base-java-image:2.1.0

RUN yum update -y && \
    yum groupinstall -y "Development Tools" && \
    yum install -y openssl11-devel bzip2-devel libffi-devel zlib-devel sqlite-devel xz-devel

ENV PYTHON_VERSION=3.9.11

# Set up python
RUN git clone https://github.com/pyenv/pyenv.git ~/.pyenv
ENV PYENV_ROOT /root/.pyenv
ENV PATH ${PYENV_ROOT}/shims:${PYENV_ROOT}/bin:$PATH
RUN pyenv install ${PYTHON_VERSION} && pyenv global ${PYTHON_VERSION}
