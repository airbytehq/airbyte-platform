FROM airbyte/airbyte-base-java-image:1.0

EXPOSE 8002

ENV APPLICATION cloud-server

WORKDIR /app

ARG VERSION=dev-8c5398d930

COPY bin/${APPLICATION}-${VERSION}.tar ${APPLICATION}.tar

RUN tar xf ${APPLICATION}.tar --strip-components=1

# wait for upstream dependencies to become available before starting server
ENTRYPOINT ["/bin/bash", "-c", "bin/${APPLICATION}"]
