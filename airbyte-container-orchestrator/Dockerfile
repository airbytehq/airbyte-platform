FROM airbyte/airbyte-base-java-worker:1.0.0

# Don't change this manually.  Bump version expects to make moves based on this string
ARG VERSION=0.50.48

ENV APPLICATION airbyte-container-orchestrator
ENV VERSION=${VERSION}

WORKDIR /app

COPY WellKnownTypes.json /app

# Move orchestrator app
ADD airbyte-app.tar /app

# wait for upstream dependencies to become available before starting server
ENTRYPOINT ["/bin/bash", "-c", "/app/airbyte-app/bin/${APPLICATION}"]
