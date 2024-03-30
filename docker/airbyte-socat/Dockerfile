ARG SOCAT_IMAGE_VERSION=1.7.4.3-r0
FROM alpine/socat:${SOCAT_IMAGE_VERSION}

RUN adduser -u 1000 -s /bin/sh -D airbyte

USER airbyte:airbyte
