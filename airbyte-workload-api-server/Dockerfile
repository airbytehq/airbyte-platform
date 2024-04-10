ARG JDK_IMAGE=airbyte/airbyte-base-java-image:3.2.0
FROM ${JDK_IMAGE} AS server
EXPOSE 8007 5005
ENV APPLICATION airbyte-workload-api-server
ENV VERSION ${VERSION}
WORKDIR /app

# This is automatically unzipped by Docker
USER root
ADD airbyte-app.tar /app
RUN chown -R airbyte:airbyte /app
USER airbyte:airbyte

# wait for upstream dependencies to become available before starting server
ENTRYPOINT ["/bin/bash", "-c", "airbyte-app/bin/${APPLICATION}"]
