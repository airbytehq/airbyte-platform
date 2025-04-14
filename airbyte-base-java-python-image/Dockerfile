ARG AIRBYTE_BASE_JAVA_IMAGE_TAG=dev

FROM airbyte/airbyte-base-java-image:${AIRBYTE_BASE_JAVA_IMAGE_TAG}

USER root

RUN <<EOF
yum update -y
yum groupinstall -y "Development Tools"
yum install -y gcc make patch zlib-devel bzip2 bzip2-devel readline-devel sqlite sqlite-devel openssl11-devel tk-devel libffi-devel xz-devel cargo
yum clean all
rm -rf /var/cache/yum
EOF

# Set up python
USER airbyte:airbyte
ENV PYTHON_VERSION=3.10.17
ENV PYENV_ROOT=/home/airbyte/.pyenv
ENV PATH=${PYENV_ROOT}/shims:${PYENV_ROOT}/bin:$PATH

RUN <<EOF
git clone https://github.com/pyenv/pyenv.git ~/.pyenv
pyenv install -v ${PYTHON_VERSION} && pyenv global ${PYTHON_VERSION}
python -m pip install --upgrade pip setuptools
EOF
