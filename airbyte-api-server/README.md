## Airbyte API Server

**Purpose:**

- Enables users to control Airbyte programmatically and use with Orchestration tools (ex: Airflow, Dagster, Prefect)
- Exists for Airbyte users to write applications against and enable [Powered by Airbyte](https://airbyte.com/embed-airbyte-connectors-with-api) (
  Headless version and UI version)

**Documentation**

Documentation for the API can be found at https://reference.airbyte.com/ and is powered by readme.io. The documentation currently only fully supports
Airbyte Cloud. We will have pages specific to OSS Airbtye Instances soon!

The main differences will be configuration inputs for the source and destination create endpoints.
OAuth endpoints and workspace updates are not supported in OSS currently.

**Local Airbyte Instance Usage**

*Docker Compose*

If your instance of Airbyte is running locally using docker-compose, you can access the Airbyte API of the local instance by spinning up with the
latest docker compose files. You can then make a call to `http://localhost:8006/v1/<endpoint>` or the health endpoint
at `http://localhost:8006/health`. Calls to the Airbyte API through docker compose will go through the airbyte-proxy, which requires basic auth with a
user and password supplied in the .env files.

*Kubernetes*

If you are running an instance of Airbyte locally using kubernetes, you can access the Airbyte API of the local instance by:

1. Setting up a port forward to the airbyte-api-server kube svc by running `kubectl port-forward svc/airbyte-airbyte-api-server-svc 8006:80 &`
2. Making a call to `http://localhost:8006/v1/<endpoint>` or the health endpoint at `http://localhost:8006/health`.
