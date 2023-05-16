ARG BASE_IMAGE=airbyte/airbyte-base-java-image:2.0.2
# Alpine amazoncorretto:19.0.2-alpine3.17
FROM ${BASE_IMAGE}

ARG DOCKER_BUILD_ARCH=amd64

WORKDIR /app

RUN if [ -f /etc/os-release ]; then \
      . /etc/os-release && \
      echo $ID && \
      if [[ "$ID" == "alpine" ]]; then \
        echo "Building for Alpine" && \
        apk add tar && \
        apk update && apk upgrade && apk upgrade -U openssl && \
        apk del openldap; \
      elif [[ "$ID" == "amzn" ]]; then \
        yum install -y tar;  \
      fi \
    fi

# Add the Datadog Java APM agent
ADD https://dtdg.co/latest-java-tracer dd-java-agent.jar

# Add the OpenTelemetry Java APM agent
ADD https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/latest/download/opentelemetry-javaagent.jar opentelemetry-javaagent.jar
