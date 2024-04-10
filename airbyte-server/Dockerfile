ARG JDK_IMAGE=airbyte/airbyte-base-java-image:3.2.0
FROM ${JDK_IMAGE} AS server

EXPOSE 8000 5005

ARG VERSION=dev

ENV APPLICATION airbyte-server
ENV VERSION ${VERSION}

WORKDIR /app

# This is automatically unzipped by Docker
USER root
ADD airbyte-app.tar /app
RUN chown -R airbyte:airbyte /app
USER airbyte:airbyte

# wait for upstream dependencies to become available before starting server
ENTRYPOINT ["/bin/bash", "-c", "airbyte-app/bin/${APPLICATION}"]
