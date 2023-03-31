ARG JDK_IMAGE=airbyte/airbyte-base-java-image:2.0.1
FROM ${JDK_IMAGE}

ARG VERSION=0.42.1

ENV APPLICATION airbyte-bootloader
ENV VERSION ${VERSION}

WORKDIR /app

ADD bin/${APPLICATION}-${VERSION}.tar /app


ENTRYPOINT ["/bin/bash", "-c", "${APPLICATION}-${VERSION}/bin/${APPLICATION}"]
