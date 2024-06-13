ARG ALPINE_IMAGE_VERSION=3.18
FROM alpine:${ALPINE_IMAGE_VERSION}

RUN adduser -u 1000 -s /bin/sh -D airbyte

RUN wget https://dl.min.io/client/mc/release/linux-amd64/mc && \
    mv mc /usr/bin/mc && \
    chmod +x /usr/bin/mc

USER airbyte:airbyte
