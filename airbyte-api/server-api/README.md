# server-api

Defines the OpenApi configuration for the Airbyte Configuration API. It also is responsible for generating the following from the API spec:
* Kotlin API client
* Java API server - this generated code is used in `airbyte-server` to allow us to implement the Configuration API in a type safe way. See `ConfigurationApi.java` in `airbyte-server`
* API docs

## Key Files
* src/openapi/config.yaml - Defines the config API interface using OpenApi3
* AirbyteApiClient.kt - wraps all api clients so that they can be dependency injected together

## Configuration

In order to use the `AirbyteApiClient` in an application, you must provide the following configuration
in the application's configuration file (`application.yml`):

```yaml
airbyte:
  internal-api:
    base-path: ${INTERNAL_API_HOST}/api
    connect-timeout-seconds: ${AIRBYTE_API_CONNECT_TIMEOUT_SECONDS:30}
    read-timeout-seconds: ${AIRBYTE_API_READ_TIMEOUT_SECONDS:600}
    retries:
      delay-seconds: ${AIRBYTE_API_RETRY_DELAY_SECONDS:2}
      max: ${AIRBYTE_API_MAX_RETRIES:5}
```

where `INTERNAL_API_HOST` should be the full URL with scheme and port if necessary (e.g. `https://cloud.airbyte.io:443`).  All other
environment variables are optional and will use default values from the above configuration if not provided.  Below is a description
of each:

| Environment Variable| Description |
|:----|:----|
| AIRBYTE_API_CONNECT_TIMEOUT_SECONDS | The Airbyte API connection timeout in seconds. |
| AIRBYTE_API_MAX_RETRIES | The maximum number of client retries to attempt before failing the request |
| AIRBYTE_API_READ_TIMEOUT_SECONDS | The Airbyte API read timeout in seconds. |
| AIRBYTE_API_RETRY_DELAY_SECONDS | The client retry delay in seconds when handling a retryable exception. |
| INTERNAL_API_HOST | The full URL with scheme and port for the API. |

If the proper configuration is provided, the `AirbyteApiClient` can be injected for use in the application's code:

```kotlin
fun getWorkspace(workspaceId: UUID): WorkspaceRead {
    return airbyteApiClient.workspaceApi.getWorkspace(
        WorkspaceIdRequestBody(workspaceId = workspaceId, includeTombstone = true)
    )
}
```