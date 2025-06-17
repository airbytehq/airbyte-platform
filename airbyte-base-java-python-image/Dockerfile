ARG AIRBYTE_BASE_JAVA_IMAGE_TAG=dev

FROM airbyte/airbyte-base-java-image:${AIRBYTE_BASE_JAVA_IMAGE_TAG}

USER root

RUN <<EOF
yum update -y --security
yum groupinstall -y "Development Tools"
yum install -y gcc make patch zlib-devel bzip2 bzip2-devel readline-devel sqlite sqlite-devel openssl11-devel tk-devel libffi-devel xz-devel cargo
yum clean all
rm -rf /var/cache/yum
# Remaking these as owned by airbyte in the case where the above update resets the ownership to root.
# This is copied from the airbyte-base-java-image.
chown -R airbyte:airbyte /usr/share/pki/ca-trust-source
chown -R airbyte:airbyte /etc/pki/ca-trust
EOF

# Set up python
USER airbyte:airbyte
ENV PYTHON_VERSION=3.10.18
ENV PYENV_ROOT=/home/airbyte/.pyenv
ENV PATH=${PYENV_ROOT}/shims:${PYENV_ROOT}/bin:$PATH

RUN <<EOF
git clone https://github.com/pyenv/pyenv.git ~/.pyenv
pyenv install -v ${PYTHON_VERSION} && pyenv global ${PYTHON_VERSION}
python -m pip install --upgrade pip setuptools
EOF
