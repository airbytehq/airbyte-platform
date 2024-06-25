ARG JDK_IMAGE=airbyte/airbyte-base-java-image:3.2.3

FROM ${JDK_IMAGE}
EXPOSE 8007 5005
ENV APPLICATION airbyte-workload-api-server
ENV VERSION ${VERSION}

WORKDIR /app
ADD --chown=airbyte:airbyte airbyte-app.tar /app
USER airbyte:airbyte

ENTRYPOINT ["/bin/bash", "-c", "airbyte-app/bin/${APPLICATION}"]
