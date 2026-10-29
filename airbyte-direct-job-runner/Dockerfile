ARG JAVA_BASE_IMAGE_VERSION=3.3

FROM scratch as builder
WORKDIR /app
ADD airbyte-app.tar /app

FROM airbyte/airbyte-base-java-image:${JAVA_BASE_IMAGE_VERSION}

# 8080 is the port micronaut listens on
# 8085 is the Micronaut management endpoint port
# 5005 is the remote debug port
EXPOSE 8080 8085 5005

ARG VERSION=dev

ENV APPLICATION airbyte-direct-job-runner
ENV VERSION=${VERSION}

WORKDIR /app
COPY --chown=airbyte:airbyte --from=builder /app /app
USER airbyte:airbyte

CMD ["/bin/bash", "-c", "/app/airbyte-app/bin/${APPLICATION}"]
