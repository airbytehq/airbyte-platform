ARG JAVA_WORKER_BASE_IMAGE_VERSION=2.1.0
FROM airbyte/airbyte-base-java-worker-image:${JAVA_WORKER_BASE_IMAGE_VERSION}

ENV APPLICATION airbyte-workload-launcher
ENV VERSION ${VERSION}

WORKDIR /app

# This is automatically unzipped by Docker
USER root
ADD airbyte-app.tar /app
RUN chown -R airbyte:airbyte /app
USER airbyte:airbyte

# 8016 is the port micronaut listens on
# 5005 is the remote debug port
EXPOSE 8016 5005

# wait for upstream dependencies to become available before starting server
ENTRYPOINT ["/bin/bash", "-c", "airbyte-app/bin/${APPLICATION}"]
