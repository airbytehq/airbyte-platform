ARG JDK_IMAGE=airbyte/airbyte-base-java-image:3.3.3
FROM ${JDK_IMAGE}

ARG TARGETPLATFORM

USER root

RUN <<EOF
mkdir -p /tmp/workspace
chown -R airbyte:airbyte /tmp/workspace
mkdir -p /tmp/airbyte_local
chown -R airbyte:airbyte /tmp/airbyte_local
EOF

USER airbyte:airbyte
