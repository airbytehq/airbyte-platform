FROM amazoncorretto:21-alpine

ARG DOCKER_BUILD_ARCH=amd64

WORKDIR /app

# Use apk for Alpine Linux to install necessary packages
RUN <<EOF
apk --no-cache add tar shadow bash
groupadd --gid 1000 airbyte
useradd --uid 1000 --gid airbyte --shell /bin/bash --create-home airbyte
# Create mount point for secrets
mkdir /secrets
chown -R airbyte:airbyte /secrets
chown -R airbyte:airbyte /app
EOF

# Add the Datadog Java APM agent
ADD --chown=airbyte:airbyte https://dtdg.co/latest-java-tracer dd-java-agent.jar

# Add the OpenTelemetry Java APM agent
ADD --chown=airbyte:airbyte https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/latest/download/opentelemetry-javaagent.jar opentelemetry-javaagent.jar

USER airbyte:airbyte
