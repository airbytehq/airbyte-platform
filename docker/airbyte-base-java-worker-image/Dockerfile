ARG JDK_IMAGE=airbyte/airbyte-base-java-image:3.2.1
FROM ${JDK_IMAGE}

ARG TARGETPLATFORM

USER root

RUN <<EOF
amazon-linux-extras install -y docker
yum install -y jq tar
yum clean all
rm -rf /var/cache/yum

curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/$TARGETPLATFORM/kubectl"
chmod +x kubectl
mv kubectl /usr/local/bin/

mkdir -p /tmp/workspace
chown -R airbyte:airbyte /tmp/workspace
mkdir -p /tmp/airbyte_local
chown -R airbyte:airbyte /tmp/airbyte_local
EOF

USER airbyte:airbyte
