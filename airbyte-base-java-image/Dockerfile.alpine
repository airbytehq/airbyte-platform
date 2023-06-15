FROM amazoncorretto:19.0.2-alpine3.17

ARG DOCKER_BUILD_ARCH=amd64

WORKDIR /app

RUN apk add tar && \
    apk update && apk upgrade && apk upgrade -U openssl && \
    apk del openldap

# Add the Datadog Java APM agent
ADD https://dtdg.co/latest-java-tracer dd-java-agent.jar

# Add the OpenTelemetry Java APM agent
ADD https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/latest/download/opentelemetry-javaagent.jar opentelemetry-javaagent.jar
