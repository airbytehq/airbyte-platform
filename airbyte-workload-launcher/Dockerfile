FROM airbyte/airbyte-base-java-worker:1.0.0

ENV APPLICATION airbyte-workload-launcher
ENV VERSION ${VERSION}

WORKDIR /app

# This is automatically unzipped by Docker
ADD airbyte-app.tar /app

# 8016 is the port micronaut listens on
# 5005 is the remote debug port
EXPOSE 8016 5005

# wait for upstream dependencies to become available before starting server
ENTRYPOINT ["/bin/bash", "-c", "airbyte-app/bin/${APPLICATION}"]
