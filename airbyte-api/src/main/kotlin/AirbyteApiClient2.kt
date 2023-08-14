package io.airbyte.api.client2

import dev.failsafe.RetryPolicy
import io.airbyte.api.client2.generated.AttemptApi
import io.airbyte.api.client2.generated.ConnectionApi
import io.airbyte.api.client2.generated.ConnectorBuilderProjectApi
import io.airbyte.api.client2.generated.DestinationApi
import io.airbyte.api.client2.generated.DestinationDefinitionApi
import io.airbyte.api.client2.generated.DestinationDefinitionSpecificationApi
import io.airbyte.api.client2.generated.HealthApi
import io.airbyte.api.client2.generated.JobRetryStatesApi
import io.airbyte.api.client2.generated.JobsApi
import io.airbyte.api.client2.generated.OperationApi
import io.airbyte.api.client2.generated.SourceApi
import io.airbyte.api.client2.generated.SourceDefinitionApi
import io.airbyte.api.client2.generated.SourceDefinitionSpecificationApi
import io.airbyte.api.client2.generated.StateApi
import io.airbyte.api.client2.generated.StreamStatusesApi
import io.airbyte.api.client2.generated.WorkspaceApi
import okhttp3.OkHttpClient

/**
 * This class wraps all the generated API clients and provides a single entry point. This class is meant
 * to consolidate all our API endpoints into a fluent-ish client. Our open API generators create a separate
 * class per API "root-route". For example, if our API has two routes "/v1/First/get" and "/v1/Second/get",
 * OpenAPI generates (essentially) the following files:
 * <p>
 * ApiClient.java, FirstApi.java, SecondApi.java
 * <p>
 * To call the API type-safely, we'd do new FirstApi(new ApiClient()).get() or new SecondApi(new
 * ApiClient()).get(), which can get cumbersome if we're interacting with many pieces of the API.
 * <p>
 * Our new JVM (kotlin) client is designed to do a few things:
 * <ol>
 * <li>1. Use kotlin!</li>
 * <li>2. Use OkHttp3 instead of the native java client (The native one dies on any network blip. OkHttp
 * is more robust and smooths over network blips).</li>
 * <li>3. Integrate failsafe (https://failsafe.dev/) for circuit breaking / retry<li>
 * policies.
 * </ol>
 * <p>
 * todo (cgardens): The LogsApi is intentionally not included because in the java client we had to do some
 * work to set the correct headers in the generated code. At some point we will need to test that that
 * functionality works in the new client (and if necessary, patch it). Context: https://github.com/airbytehq/airbyte/pull/1799
 */
@Suppress("MemberVisibilityCanBePrivate")
class AirbyteApiClient2
  @JvmOverloads
  constructor(
    basePath: String,
    policy: RetryPolicy<okhttp3.Response> = RetryPolicy.ofDefaults(),
    httpClient: OkHttpClient = OkHttpClient(),
  ) {

    val connectionApi: ConnectionApi
    val connectorBuilderProjectApi: ConnectorBuilderProjectApi
    val destinationDefinitionApi: DestinationDefinitionApi
    val destinationApi: DestinationApi
    val destinationSpecificationApi: DestinationDefinitionSpecificationApi
    val jobsApi: JobsApi
    val jobRetryStatesApi: JobRetryStatesApi
    val operationApi: OperationApi
    val sourceDefinitionApi: SourceDefinitionApi
    val sourceApi: SourceApi
    val sourceDefinitionSpecificationApi: SourceDefinitionSpecificationApi
    val workspaceApi: WorkspaceApi
    val healthApi: HealthApi
    val attemptApi: AttemptApi
    val stateApi: StateApi
    val streamStatusesApi: StreamStatusesApi

    init {
      connectionApi = ConnectionApi(basePath = basePath, client = httpClient, policy = policy)
      connectorBuilderProjectApi = ConnectorBuilderProjectApi(basePath = basePath, client = httpClient, policy = policy)
      destinationDefinitionApi = DestinationDefinitionApi(basePath = basePath, client = httpClient, policy = policy)
      destinationApi = DestinationApi(basePath = basePath, client = httpClient, policy = policy)
      destinationSpecificationApi = DestinationDefinitionSpecificationApi(basePath = basePath, client = httpClient, policy = policy)
      jobsApi = JobsApi(basePath = basePath, client = httpClient, policy = policy)
      jobRetryStatesApi = JobRetryStatesApi(basePath = basePath, client = httpClient, policy = policy)
      operationApi = OperationApi(basePath = basePath, client = httpClient, policy = policy)
      sourceDefinitionApi = SourceDefinitionApi(basePath = basePath, client = httpClient, policy = policy)
      sourceApi = SourceApi(basePath = basePath, client = httpClient, policy = policy)
      sourceDefinitionSpecificationApi = SourceDefinitionSpecificationApi(basePath = basePath, client = httpClient, policy = policy)
      workspaceApi = WorkspaceApi(basePath = basePath, client = httpClient, policy = policy)
      healthApi = HealthApi(basePath = basePath, client = httpClient, policy = policy)
      attemptApi = AttemptApi(basePath = basePath, client = httpClient, policy = policy)
      stateApi = StateApi(basePath = basePath, client = httpClient, policy = policy)
      streamStatusesApi = StreamStatusesApi(basePath = basePath, client = httpClient, policy = policy)
    }
  }
