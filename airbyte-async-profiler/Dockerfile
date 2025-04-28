ARG JAVA_BASE_IMAGE=3.3.5

FROM scratch as builder
WORKDIR /app

ADD airbyte-app.tar /app

FROM airbyte/airbyte-base-java-image:${JAVA_BASE_IMAGE}

ARG DOCKER_BUILD_ARCH=amd64

# Don't change this manually.  Bump version expects to make moves based on this string
ARG VERSION=dev

ENV APPLICATION airbyte-async-profiler
ENV VERSION=${VERSION}

COPY --chown=airbyte:airbyte --from=builder /app /app
USER airbyte:airbyte

CMD ["/bin/bash", "-c", "/app/airbyte-app/bin/${APPLICATION}"]
