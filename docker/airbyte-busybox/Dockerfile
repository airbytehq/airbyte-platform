ARG BUSYBOX_IMAGE_VERSION=1.28
FROM busybox:${BUSYBOX_IMAGE_VERSION}

RUN adduser -u 1000 -s /bin/sh -D airbyte

USER airbyte:airbyte
