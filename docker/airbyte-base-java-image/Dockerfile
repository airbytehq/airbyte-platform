FROM amazoncorretto:21

ARG DOCKER_BUILD_ARCH=amd64

WORKDIR /app

RUN <<EOF
yum install -y tar shadow-utils
yum update -y --security
yum clean all
rm -rf /var/cache/yum
groupadd --gid 1000 airbyte
useradd --uid 1000 --gid airbyte --shell /bin/bash --create-home airbyte
# Create mount point for secrets
mkdir /secrets
chown -R airbyte:airbyte /secrets
chown -R airbyte:airbyte /app
chown -R airbyte:airbyte /usr/share/pki/ca-trust-source
chown -R airbyte:airbyte /etc/pki/ca-trust
EOF

# Add the Datadog Java APM agent
ADD --chown=airbyte:airbyte https://dtdg.co/latest-java-tracer dd-java-agent.jar

# Add the OpenTelemetry Java APM agent
ADD --chown=airbyte:airbyte https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/latest/download/opentelemetry-javaagent.jar opentelemetry-javaagent.jar

ADD --chown=airbyte:airbyte entrypoint.sh /entrypoint.sh

USER airbyte:airbyte

ENTRYPOINT ["/entrypoint.sh"]
