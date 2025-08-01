ARG JDK_IMAGE=airbyte/airbyte-base-java-image:3.3

FROM scratch as builder
WORKDIR /app
ADD airbyte-app.tar /app

FROM ${JDK_IMAGE} AS server
EXPOSE 8000 5005
ARG VERSION=dev
ENV APPLICATION airbyte-server
ENV VERSION ${VERSION}
WORKDIR /app
COPY --chown=airbyte:airbyte --from=builder /app /app
USER airbyte:airbyte

# wait for upstream dependencies to become available before starting server
CMD ["/bin/bash", "-c", "airbyte-app/bin/${APPLICATION}"]
