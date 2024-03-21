FROM amazoncorretto:21

ARG DOCKER_BUILD_ARCH=amd64

RUN yum install -y tar shadow-utils

RUN groupadd --gid 1000 airbyte \
    && useradd --uid 1000 --gid airbyte --shell /bin/bash --create-home airbyte

WORKDIR /app

# Add the Datadog Java APM agent
ADD https://dtdg.co/latest-java-tracer dd-java-agent.jar

# Add the OpenTelemetry Java APM agent
ADD https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/latest/download/opentelemetry-javaagent.jar opentelemetry-javaagent.jar

# Create mount point for secrets
RUN mkdir /secrets && chown -R airbyte:airbyte /secrets

RUN chown -R airbyte:airbyte /app

USER airbyte:airbyte
