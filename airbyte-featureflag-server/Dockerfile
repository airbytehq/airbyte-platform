ARG JDK_IMAGE=airbyte/airbyte-base-java-image:3.3

FROM scratch as builder
WORKDIR /app
ADD airbyte-app.tar /app

FROM ${JDK_IMAGE}
# 8007 is the port micronaut listens on
# 5005 is the remote debug port
# 8085 is the Micronaut management endpoint port
EXPOSE 8007 5005 8085
ENV APPLICATION airbyte-featureflag-server
ENV VERSION ${VERSION}

WORKDIR /app
COPY --chown=airbyte:airbyte --from=builder /app /app
USER airbyte:airbyte

CMD ["/bin/bash", "-c", "airbyte-app/bin/${APPLICATION}"]
