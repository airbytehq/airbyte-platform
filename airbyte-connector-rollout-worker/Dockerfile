ARG JAVA_BASE_IMAGE_VERSION=3.3.6

FROM scratch as builder
WORKDIR /app
ADD airbyte-app.tar /app

FROM airbyte/airbyte-base-java-image:${JAVA_BASE_IMAGE_VERSION}

ENV APPLICATION airbyte-connector-rollout-worker
ENV VERSION ${VERSION}
# 8016 is the port micronaut listens on
# 5005 is the remote debug port
EXPOSE 8016 5005

WORKDIR /app
COPY --chown=airbyte:airbyte --from=builder /app /app
USER airbyte:airbyte

CMD ["/bin/bash", "-c", "airbyte-app/bin/${APPLICATION}"]
