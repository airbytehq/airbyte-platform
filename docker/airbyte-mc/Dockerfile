ARG ALPINE_IMAGE_VERSION=3.18
FROM alpine:${ALPINE_IMAGE_VERSION}

RUN adduser -u 1000 -s /bin/sh -D airbyte

RUN apkArch="$(apk --print-arch)"; \
    case "$apkArch" in \
        aarch64) export PLATFORM='arm64' ;; \
        *) export PLATFORM='amd64' ;; \
    esac; \
    wget https://dl.min.io/client/mc/release/linux-${PLATFORM}/mc && \
    mv mc /usr/bin/mc && \
    chmod +x /usr/bin/mc

USER airbyte:airbyte
