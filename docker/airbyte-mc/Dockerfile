ARG ALPINE_IMAGE_VERSION=3.18

FROM alpine:${ALPINE_IMAGE_VERSION}

ARG AMD64_URL=https://dl.min.io/client/mc/release/linux-amd64/archive/mc.RELEASE.2024-03-25T16-41-14Z
ARG AMD64_SHA256SUM=e8e7732b119eadf6515af8ac814e944587352b5aeaa6e64c21f126ef763129e3
ARG ARM64_URL=https://dl.min.io/client/mc/release/linux-arm64/archive/mc.RELEASE.2024-03-25T16-41-14Z
ARG ARM64_SHA256SUM=065a8fe157792c08e2759d7c4672a945a97ca701431cd61b0029bed80148286c

RUN adduser -u 1000 -s /bin/sh -D airbyte

RUN if [ "$(uname -m)" = "aarch64" ]; then \
        DOWNLOAD_URL=${ARM64_URL} ; \
        DOWNLOAD_SHA256SUM=${ARM64_SHA256SUM} ; \
    else \
        DOWNLOAD_URL=${AMD64_URL} ; \
        DOWNLOAD_SHA256SUM=${AMD64_SHA256SUM} ; \
    fi \
    && wget -O /usr/bin/mc $DOWNLOAD_URL \
    && chmod +x /usr/bin/mc \
    && echo "${DOWNLOAD_SHA256SUM}  /usr/bin/mc" | sha256sum -c

USER airbyte:airbyte
