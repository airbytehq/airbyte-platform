# Connector Builder

To whom it may concern: Prior to Airbyte OSS version `0.45.20`, the airbyte-connector-builder-server Docker image being used was written
in Python. After the rewrite of the server to Java and subsequent [release](https://github.com/airbytehq/airbyte-platform-internal/pull/6371),
we deprecated the old server and all image versions from `0.45.20` onward are backed by Java.

## Getting started

Install dependencies, compile, and build the server

The Connector Builder API server sends commands to an entrypoint of the Airbyte CDK. Therefore, to build the Connector Builder server, an installation of the CDK needs to be available and the `CDK_VERSION` environment needs to be set to a compatible version of the CDK.

The earliest compatible version of the CDK is 0.31.1.

```bash
./gradlew -p oss airbyte-connector-builder-server:build
```

To develop the server locally (without Docker), the `CDK_PYTHON` and `CDK_ENTRYPOINT` environment variables need to be set, where `CDK_PYTHON` is the path to the python interpreter you want to use, i.e. in which the CDK is installed, and `CDK_ENTRYPOINT` is the path to the Connector Builder entrypoint, located at <CDK directory/connector_builder/main.py>.
```bash
export CDK_PYTHON=<path_to_CDK_virtual_environment>
export CDK_ENTRYPOINT=<path_to_CDK_connector_builder_main.py>
```

Example commands:
```
export CDK_PYTHON=~/code/airbyte/airbyte-cdk/python/.venv/bin/python
export CDK_ENTRYPOINT=~/code/airbyte/airbyte-cdk/python/airbyte_cdk/connector_builder/main.py
```

Then run the server (You can also do this w/o build)
```bash
./gradlew -p oss airbyte-connector-builder-server:run
```

The server is now reachable on localhost:8080

If you want to run the full platform with this local instance, you must edit the `.env` file as follows:

``` bash
# replace this
CONNECTOR_BUILDER_SERVER_API_HOST=http://airbyte-connector-builder-server:8080

# with this
CONNECTOR_BUILDER_SERVER_API_HOST=http://host.docker.internal:8080
```

Note: there are two different, but very similarly-named, environment variables; you must edit `CONNECTOR_BUILDER_SERVER_API_HOST`, not `CONNECTOR_BUILDER_API_HOST`.

### Running the platform with support for custom components (docker-compose only)

1. Run the OSS platform locally with builder docker-compose extension
    1. Example command: PATH_TO_CONNECTORS=/Users/alex/code/airbyte/airbyte-integrations/connectors docker compose -f docker-compose.yaml -f docker-compose.builder.yaml up
    2. Where PATH_TO_CONNECTORS points to the airbyte-integrations/connectors subdirectory in the opensource airbyte repository
2. Open the connector builder and develop your connector
3. When needing a custom componentt:
    1. Switch to the YAML view
    2. Define the custom component
4. Write the custom components and its unit tests
5. Run test read

Note that connector modules are added to the path at startup time. The platform instance must be restarted if you add a new connector module.

Follow these additional instructions if the connector requires 3rd party libraries that are not available in the CDK:

Developing connectors that require 3rd party libraries can be done by running the connector-builder-server locally and pointing to a custom virtual environment.

1. Create a virtual environment and install the CDK + any 3rd party library required
2. export CDK_PYTHON=<path_to_virtual_environment>
 - `CDK_PYTHON` should point to the virtual environment's python executable (example: `export CDK_PYTHON=~/code/airbyte/airbyte-cdk/python/.venv/bin/python`)
3. export CDK_ENTRYPOINT=<path_to_CDK_connector_builder_main.py>
4. ./gradlew -p oss airbyte-connector-builder-server:run
    1. The server is now reachable on localhost:8080
5. Update the server to point to port 8080 by editing .env and replacing
    
    ```
    CONNECTOR_BUILDER_SERVER_API_HOST=http://airbyte-connector-builder-server:8080
    ```
    with
    ```
    CONNECTOR_BUILDER_SERVER_API_HOST=http://host.docker.internal:8080
    ```
    
6. Follow the standard instructions

## OpenAPI generation

Run it via Gradle by running this from the Airbyte project root:
```bash
./gradlew -p oss airbyte-connector-builder-server:generateOpenApiServer
```

## Changing the used CDK version

TODO: The current `connector-builder-server` and `airbyte-webapp` must stay in sync using the same version of
the `airbyte-cdk` package. We can specify this as an environment variable in the top-level `.env` file. 
This has not been implemented yet, but we may also need to implement this in a follow-up change.

## Sequence diagram for handling Connector Builder requests
![img.png](img.png)
