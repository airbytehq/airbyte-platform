FROM airbyte/airbyte-base-java-image:1.0

WORKDIR /airbyte

ENV APPLICATION cloud-bootloader

ARG VERSION=dev-8c5398d930

COPY bin/${APPLICATION}-${VERSION}.tar ${APPLICATION}.tar

RUN tar xf ${APPLICATION}.tar --strip-components=1

ENTRYPOINT ["/bin/bash", "-c", "bin/${APPLICATION}"]
