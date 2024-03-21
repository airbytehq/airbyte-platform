ARG JAVA_WORKER_BASE_IMAGE_VERSION=2.1.0
FROM airbyte/airbyte-base-java-worker-image:${JAVA_WORKER_BASE_IMAGE_VERSION}

ENV APPLICATION airbyte-workers
ENV VERSION ${VERSION}

WORKDIR /app

USER root
COPY WellKnownTypes.json /app

# Move worker app
ADD airbyte-app.tar /app
RUN chown -R airbyte:airbyte /app
USER airbyte:airbyte

EXPOSE 5005

# wait for upstream dependencies to become available before starting server
ENTRYPOINT ["/bin/bash", "-c", "airbyte-app/bin/${APPLICATION}"]
