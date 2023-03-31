ARG JDK_IMAGE=airbyte/airbyte-base-java-image:2.0.1
FROM ${JDK_IMAGE} AS metrics-reporter

ARG VERSION=0.42.1

ENV APPLICATION airbyte-metrics-reporter
ENV VERSION ${VERSION}

WORKDIR /app

ADD bin/${APPLICATION}-${VERSION}.tar /app


# wait for upstream dependencies to become available before starting server
ENTRYPOINT ["/bin/bash", "-c", "${APPLICATION}-${VERSION}/bin/${APPLICATION}"]
