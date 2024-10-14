# Connector Builder

To whom it may concern: Prior to Airbyte OSS version `0.45.20`, the airbyte-connector-builder-server Docker image being used was written
in Python. After the rewrite of the server to Java and subsequent [release](https://github.com/airbytehq/airbyte-platform-internal/pull/6371),
we deprecated the old server and all image versions from `0.45.20` onward are backed by Java.

## Getting started

### Install CDK dependencies

The Connector Builder API server sends commands to an entrypoint of the Airbyte CDK. Therefore, to build the Connector Builder server, an installation of the CDK needs to be available and the `CDK_VERSION` environment needs to be set to a compatible version of the CDK. The earliest compatible version of the CDK is 0.31.1.

To set up a local CDK environment, navigate to the airbyte-cdk/python folder in your local airbyte repo and create your CDK environment:

```bash
poetry install
```

If `poetry` cannot be found, run `pip install poetry` first.

You will need to add the path to this environment in the next steps. You can run `poetry show -v` to verify it.

#### Setting your Python version

The Python CDK is not currently compatible with the latest versions of Python. If you are using an incompatible version Python, we recommend using `pyenv` to manage the Python version locally:

```bash
pyenv local 3.10
poetry env use $(pyenv which python)
poetry install
```

### Compile, build and run the server

From the root folder of your local airbyte-platform-internal repo, run the build command:

```bash
./gradlew :oss:airbyte-connector-builder-server:build
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
./gradlew :oss:airbyte-connector-builder-server:run
```

If you experience any issues, try running the full command as:

```bash
sudo CDK_PYTHON=<path_to_CDK_virtual_environment> CDK_ENTRYPOINT=<path_to_CDK_connector_builder_main.py> ./gradlew :oss:airbyte-connector-builder-server:run
```
(note that you will need to use full paths in this case, i.e. not starting with `~`)

The server is now reachable on localhost:8080

### Run the full platform locally

If you want to run the full platform locally using your local python CDK code, first follow the steps laid out above to get a locally-running gradle builder server pointing at your local CDK code. 

You must then make two changes:
1. The `airbyte-server` makes requests to the builder server (e.g. read_stream), so these must be redirected to the locally-running gradle builder server by editing the `oss/charts/airbyte/templates/env-configmap.yaml` as follows:
    ```bash
    # replace this
    CONNECTOR_BUILDER_SERVER_API_HOST: http://{{ .Release.Name }}-airbyte-connector-builder-server-svc:{{ index .Values "connector-builder-server" "service" "port" }}
    
    # with this
    CONNECTOR_BUILDER_SERVER_API_HOST: http://host.docker.internal:8080
    ```
    Then recreate your local deploy with
    ```bash
    make dev.up.oss
    ```
2. The `airbyte-webapp` also makes requests directly to the builder server (e.g. resolve_manifest), so these must be redirected to the locally-running gradle builder server by editing the `oss/airbyte-webapp/.env.development-k8s` as follows:
    ```bash
    # replace this
    REACT_APP_CONNECTOR_BUILDER_API_URL=https://local.airbyte.dev/connector-builder-api
    
    # with this
    REACT_APP_CONNECTOR_BUILDER_API_URL=http://localhost:8080
    ```
    Then restart your local dev webapp with
    ```
    cd oss/airbyte-webapp
    pnpm install
    pnpm start oss-k8s
    ```

If you then go to https://localhost:3000/ in your browser, and navigate to the connector builder, all requests directed toward the connector builder server will now be sent to your locally-running gradle builder server that uses your local CDK code.

From this point forward, you can simply edit python files under the `airbyte-cdk/python` directory, and those changes will immediately be reflected in any requests that are sent from the Builder UI.

### Running the platform with support for custom components

1. Run the OSS platform locally with the PATH_TO_CONNECTORS env var set
    1. Example command: PATH_TO_CONNECTORS=$HOME/Developer/github/airbytehq/airbyte/airbyte-integrations/connectors make deploy.oss
    2. Where PATH_TO_CONNECTORS points to the airbyte-integrations/connectors subdirectory in the opensource airbyte repository
2. Open the connector builder and develop your connector
3. When needing a custom componentt:
    1. Switch to the YAML view
    2. Define the custom component
4. Write the custom components and its unit tests
5. Run test read

Note that connector modules are added to the path at startup time. The platform instance must be restarted if you add a new connector module.

Also note that this approach is not currently compatible with the above approach to use your local CDK code, so it is not currently possible to run with both local CDK code and custom component support. 

### Developing in a K8S Deployment
_**⚠️ Warning**: Using Local CDKs, or Custom Components is not supported at this time_

_**💡 When to use**: You are modifying the API (spec/controller/handler) and want a faster iteration cycle_

#### Build and Deploy
Ensure you have k8s running
```bash
make vm.up
make build.oss
make deploy.oss
```

If successful you should have an OSS instance available on your machine at https://local.airbyte.dev

#### Fast Reload Changes
After you have made your modifications to the builder server you can "warm" reload the changes into the k8s cluster via

```bash
./gradlew :oss:airbyte-connector-builder-server:kubeReload
```

you can also do this for the regular airbyte server

```bash
./gradlew :oss:airbyte-server:kubeReload
```
## OpenAPI generation

Run it via Gradle by running this from the Airbyte project root:
```bash
./gradlew :oss:airbyte-connector-builder-server:generateOpenApiServer
```

## Changing the used CDK version

TODO: The current `connector-builder-server` and `airbyte-webapp` must stay in sync using the same version of
the `airbyte-cdk` package. We can specify this as an environment variable in the top-level `.env` file. 
This has not been implemented yet, but we may also need to implement this in a follow-up change.

## Sequence diagram for handling Connector Builder requests
![img.png](img.png)
