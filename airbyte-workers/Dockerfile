ARG JAVA_WORKER_BASE_IMAGE_VERSION=2.2.0

FROM scratch as builder
WORKDIR /app
ADD airbyte-app.tar /app

FROM airbyte/airbyte-base-java-worker-image:${JAVA_WORKER_BASE_IMAGE_VERSION}
ENV APPLICATION airbyte-workers
ENV VERSION ${VERSION}
EXPOSE 5005
WORKDIR /app

COPY --chown=airbyte:airbyte WellKnownTypes.json /app
COPY --chown=airbyte:airbyte --from=builder /app /app
USER airbyte:airbyte

ENTRYPOINT ["/bin/bash", "-c", "airbyte-app/bin/${APPLICATION}"]
