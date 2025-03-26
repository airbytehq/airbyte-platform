/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client

import dev.failsafe.RetryPolicy
import io.airbyte.api.client.generated.ActorDefinitionVersionApi
import io.airbyte.api.client.generated.AttemptApi
import io.airbyte.api.client.generated.ConnectionApi
import io.airbyte.api.client.generated.ConnectorBuilderProjectApi
import io.airbyte.api.client.generated.ConnectorRolloutApi
import io.airbyte.api.client.generated.DataplaneApi
import io.airbyte.api.client.generated.DeploymentMetadataApi
import io.airbyte.api.client.generated.DestinationApi
import io.airbyte.api.client.generated.DestinationDefinitionApi
import io.airbyte.api.client.generated.DestinationDefinitionSpecificationApi
import io.airbyte.api.client.generated.HealthApi
import io.airbyte.api.client.generated.JobRetryStatesApi
import io.airbyte.api.client.generated.JobsApi
import io.airbyte.api.client.generated.OperationApi
import io.airbyte.api.client.generated.OrganizationApi
import io.airbyte.api.client.generated.OrganizationPaymentConfigApi
import io.airbyte.api.client.generated.PermissionApi
import io.airbyte.api.client.generated.SecretsPersistenceConfigApi
import io.airbyte.api.client.generated.SignalApi
import io.airbyte.api.client.generated.SourceApi
import io.airbyte.api.client.generated.SourceDefinitionApi
import io.airbyte.api.client.generated.SourceDefinitionSpecificationApi
import io.airbyte.api.client.generated.StateApi
import io.airbyte.api.client.generated.StreamStatusesApi
import io.airbyte.api.client.generated.UserApi
import io.airbyte.api.client.generated.WebBackendApi
import io.airbyte.api.client.generated.WorkspaceApi
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import okhttp3.OkHttpClient
import okhttp3.Response

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
 *
 * This needs to be open so that we can wrap it in micronaut test annotations for mock injection in tests.
 */
@Suppress("MemberVisibilityCanBePrivate")
@Singleton
@Requires(property = "airbyte.internal-api.base-path")
open class AirbyteApiClient(
  @Value("\${airbyte.internal-api.base-path}") basePath: String,
  @Named("airbyteApiClientRetryPolicy") policy: RetryPolicy<Response>,
  @Named("airbyteApiOkHttpClient") httpClient: OkHttpClient,
) {
  val actorDefinitionVersionApi = ActorDefinitionVersionApi(basePath = basePath, client = httpClient, policy = policy)
  val attemptApi = AttemptApi(basePath = basePath, client = httpClient, policy = policy)
  val connectionApi = ConnectionApi(basePath = basePath, client = httpClient, policy = policy)
  val connectorBuilderProjectApi = ConnectorBuilderProjectApi(basePath = basePath, client = httpClient, policy = policy)
  val connectorRolloutApi = ConnectorRolloutApi(basePath = basePath, client = httpClient, policy = policy)
  val dataplaneApi = DataplaneApi(basePath = basePath, client = httpClient, policy = policy)
  val deploymentMetadataApi = DeploymentMetadataApi(basePath = basePath, client = httpClient, policy = policy)
  val destinationApi = DestinationApi(basePath = basePath, client = httpClient, policy = policy)
  val destinationDefinitionApi = DestinationDefinitionApi(basePath = basePath, client = httpClient, policy = policy)
  val destinationDefinitionSpecificationApi =
    DestinationDefinitionSpecificationApi(basePath = basePath, client = httpClient, policy = policy)
  val healthApi = HealthApi(basePath = basePath, client = httpClient, policy = policy)
  val jobsApi = JobsApi(basePath = basePath, client = httpClient, policy = policy)
  val jobRetryStatesApi = JobRetryStatesApi(basePath = basePath, client = httpClient, policy = policy)
  val operationApi = OperationApi(basePath = basePath, client = httpClient, policy = policy)
  val organizationApi = OrganizationApi(basePath = basePath, client = httpClient, policy = policy)
  val organizationPaymentConfigApi = OrganizationPaymentConfigApi(basePath = basePath, client = httpClient, policy = policy)
  val permissionApi = PermissionApi(basePath = basePath, client = httpClient, policy = policy)
  val secretPersistenceConfigApi = SecretsPersistenceConfigApi(basePath = basePath, client = httpClient, policy = policy)
  val signalApi = SignalApi(basePath = basePath, client = httpClient, policy = policy)
  val sourceApi = SourceApi(basePath = basePath, client = httpClient, policy = policy)
  val sourceDefinitionApi = SourceDefinitionApi(basePath = basePath, client = httpClient, policy = policy)
  val sourceDefinitionSpecificationApi =
    SourceDefinitionSpecificationApi(basePath = basePath, client = httpClient, policy = policy)
  val stateApi = StateApi(basePath = basePath, client = httpClient, policy = policy)
  val streamStatusesApi = StreamStatusesApi(basePath = basePath, client = httpClient, policy = policy)
  val userApi = UserApi(basePath = basePath, client = httpClient, policy = policy)
  val webBackendApi = WebBackendApi(basePath = basePath, client = httpClient, policy = policy)
  val workspaceApi = WorkspaceApi(basePath = basePath, client = httpClient, policy = policy)
}
