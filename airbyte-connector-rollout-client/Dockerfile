ARG JAVA_BASE_IMAGE_VERSION=3.3

FROM scratch as builder
WORKDIR /app
ADD airbyte-app.tar /app

FROM airbyte/airbyte-base-java-image:${JAVA_BASE_IMAGE_VERSION}

ENV APPLICATION airbyte-connector-rollout-client
ENV VERSION ${VERSION}
# 5005 is the remote debug port
EXPOSE 5005

WORKDIR /app
COPY --chown=airbyte:airbyte --from=builder /app /app
USER airbyte:airbyte

CMD ["/bin/bash", "-c", "airbyte-app/bin/${APPLICATION}"]
