FROM airbyte/java-datadog-tracer-base:3.2

WORKDIR /airbyte

ENV APPLICATION billing

ARG VERSION=dev-8c5398d930

COPY bin/${APPLICATION}-${VERSION}.tar ${APPLICATION}.tar

RUN tar xf ${APPLICATION}.tar --strip-components=1

ENTRYPOINT ["/bin/bash", "-c", "bin/${APPLICATION}"]
