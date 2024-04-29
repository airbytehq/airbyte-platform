/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.api.client.generated.AttemptApi;
import io.airbyte.api.client.generated.ConnectionApi;
import io.airbyte.api.client.generated.ConnectorBuilderProjectApi;
import io.airbyte.api.client.generated.DeploymentMetadataApi;
import io.airbyte.api.client.generated.DestinationApi;
import io.airbyte.api.client.generated.DestinationDefinitionApi;
import io.airbyte.api.client.generated.DestinationDefinitionSpecificationApi;
import io.airbyte.api.client.generated.HealthApi;
import io.airbyte.api.client.generated.JobRetryStatesApi;
import io.airbyte.api.client.generated.JobsApi;
import io.airbyte.api.client.generated.OperationApi;
import io.airbyte.api.client.generated.OrganizationApi;
import io.airbyte.api.client.generated.PermissionApi;
import io.airbyte.api.client.generated.SecretsPersistenceConfigApi;
import io.airbyte.api.client.generated.SourceApi;
import io.airbyte.api.client.generated.SourceDefinitionApi;
import io.airbyte.api.client.generated.SourceDefinitionSpecificationApi;
import io.airbyte.api.client.generated.StateApi;
import io.airbyte.api.client.generated.StreamStatusesApi;
import io.airbyte.api.client.generated.UserApi;
import io.airbyte.api.client.generated.WebBackendApi;
import io.airbyte.api.client.generated.WorkspaceApi;
import io.airbyte.api.client.invoker.generated.ApiClient;
import java.util.Random;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DEPRECATED. USE {@link io.airbyte.api.client2.AirbyteApiClient}.
 * <p>
 * This class is meant to consolidate all our API endpoints into a fluent-ish client. Currently, all
 * open API generators create a separate class per API "root-route". For example, if our API has two
 * routes "/v1/First/get" and "/v1/Second/get", OpenAPI generates (essentially) the following files:
 * <p>
 * ApiClient.java, FirstApi.java, SecondApi.java
 * <p>
 * To call the API type-safely, we'd do new FirstApi(new ApiClient()).get() or new SecondApi(new
 * ApiClient()).get(), which can get cumbersome if we're interacting with many pieces of the API.
 * <p>
 * This is currently manually maintained. We could look into autogenerating it if needed.
 *
 * @deprecated Replaced by {@link io.airbyte.api.client2.AirbyteApiClient}
 */
@Deprecated
public class AirbyteApiClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(AirbyteApiClient.class);
  private static final Random RANDOM = new Random();

  public static final int DEFAULT_MAX_RETRIES = 4;
  public static final int DEFAULT_RETRY_INTERVAL_SECS = 10;
  public static final int DEFAULT_FINAL_INTERVAL_SECS = 10 * 60;

  private final AttemptApi attemptApi;
  private final ConnectionApi connectionApi;
  private final ConnectorBuilderProjectApi connectorBuilderProjectApi;
  private final DestinationApi destinationApi;
  private final DeploymentMetadataApi deploymentMetadataApi;
  private final DestinationDefinitionApi destinationDefinitionApi;
  private final DestinationDefinitionSpecificationApi destinationSpecificationApi;
  private final HealthApi healthApi;
  private final JobsApi jobsApi;
  private final JobRetryStatesApi jobRetryStatesApi;
  private final OperationApi operationApi;
  private final OrganizationApi organizationApi;
  private final PermissionApi permissionApi;
  private final SecretsPersistenceConfigApi secretPersistenceConfigApi;
  private final SourceApi sourceApi;
  private final SourceDefinitionApi sourceDefinitionApi;
  private final SourceDefinitionSpecificationApi sourceDefinitionSpecificationApi;
  private final StateApi stateApi;
  private final StreamStatusesApi streamStatusesApi;
  private final UserApi userApi;
  private final WebBackendApi webBackendApi;
  private final WorkspaceApi workspaceApi;

  public AirbyteApiClient(final ApiClient apiClient) {
    attemptApi = new AttemptApi(apiClient);
    connectionApi = new ConnectionApi(apiClient);
    connectorBuilderProjectApi = new ConnectorBuilderProjectApi(apiClient);
    deploymentMetadataApi = new DeploymentMetadataApi(apiClient);
    destinationApi = new DestinationApi(apiClient);
    destinationDefinitionApi = new DestinationDefinitionApi(apiClient);
    destinationSpecificationApi = new DestinationDefinitionSpecificationApi(apiClient);
    healthApi = new HealthApi(apiClient);
    jobsApi = new JobsApi(apiClient);
    jobRetryStatesApi = new JobRetryStatesApi(apiClient);
    operationApi = new OperationApi(apiClient);
    organizationApi = new OrganizationApi(apiClient);
    permissionApi = new PermissionApi(apiClient);
    secretPersistenceConfigApi = new SecretsPersistenceConfigApi(apiClient);
    sourceApi = new SourceApi(apiClient);
    sourceDefinitionApi = new SourceDefinitionApi(apiClient);
    sourceDefinitionSpecificationApi = new SourceDefinitionSpecificationApi(apiClient);
    stateApi = new StateApi(apiClient);
    streamStatusesApi = new StreamStatusesApi(apiClient);
    userApi = new UserApi(apiClient);
    webBackendApi = new WebBackendApi(apiClient);
    workspaceApi = new WorkspaceApi(apiClient);
  }

  public AttemptApi getAttemptApi() {
    return attemptApi;
  }

  public ConnectionApi getConnectionApi() {
    return connectionApi;
  }

  public ConnectorBuilderProjectApi getConnectorBuilderProjectApi() {
    return connectorBuilderProjectApi;
  }

  public DeploymentMetadataApi getDeploymentMetadataApi() {
    return deploymentMetadataApi;
  }

  public DestinationApi getDestinationApi() {
    return destinationApi;
  }

  public DestinationDefinitionApi getDestinationDefinitionApi() {
    return destinationDefinitionApi;
  }

  public DestinationDefinitionSpecificationApi getDestinationDefinitionSpecificationApi() {
    return destinationSpecificationApi;
  }

  public HealthApi getHealthApi() {
    return healthApi;
  }

  public JobsApi getJobsApi() {
    return jobsApi;
  }

  public JobRetryStatesApi getJobRetryStatesApi() {
    return jobRetryStatesApi;
  }

  public OperationApi getOperationApi() {
    return operationApi;
  }

  public OrganizationApi getOrganizationApi() {
    return organizationApi;
  }

  public PermissionApi getPermissionApi() {
    return permissionApi;
  }

  public SecretsPersistenceConfigApi getSecretPersistenceConfigApi() {
    return secretPersistenceConfigApi;
  }

  public SourceApi getSourceApi() {
    return sourceApi;
  }

  public SourceDefinitionApi getSourceDefinitionApi() {
    return sourceDefinitionApi;
  }

  public SourceDefinitionSpecificationApi getSourceDefinitionSpecificationApi() {
    return sourceDefinitionSpecificationApi;
  }

  public StateApi getStateApi() {
    return stateApi;
  }

  public StreamStatusesApi getStreamStatusesApi() {
    return streamStatusesApi;
  }

  public UserApi getUserApi() {
    return userApi;
  }

  public WebBackendApi getWebBackendApi() {
    return webBackendApi;
  }

  public WorkspaceApi getWorkspaceApi() {
    return workspaceApi;
  }

  /**
   * DEPRECATED: Use {@link io.airbyte.api.client2.AirbyteApiClient} instead.
   * <p>
   * Default to 4 retries with a randomised 1 - 10 seconds interval between the first two retries and
   * an 10-minute wait for the last retry.
   * <p>
   * Exceptions will be swallowed.
   *
   * @param call method to execute
   * @param desc short readable explanation of why this method is executed
   * @param <T> type of return type
   * @return value returned by method
   * @deprecated replaced by {@link io.airbyte.api.client2.AirbyteApiClient}
   */
  @Deprecated
  public static <T> T retryWithJitter(final Callable<T> call, final String desc) {
    return retryWithJitter(call, desc, DEFAULT_RETRY_INTERVAL_SECS, DEFAULT_FINAL_INTERVAL_SECS, DEFAULT_MAX_RETRIES);
  }

  /**
   * DEPRECATED: Use {@link io.airbyte.api.client2.AirbyteApiClient} instead.
   * <p>
   * Provides a simple retry wrapper for api calls. This retry behaviour is slightly different from
   * generally available retries libraries - the last retry is able to wait an interval inconsistent
   * with regular intervals/exponential backoff.
   * <p>
   * Since the primary retries use case is long-running workflows, the benefit of waiting a couple of
   * minutes as a last ditch effort to outlast networking disruption outweighs the cost of slightly
   * longer jobs.
   * <p>
   * Exceptions will be swallowed.
   *
   * @param call method to execute
   * @param desc short readable explanation of why this method is executed
   * @param jitterMaxIntervalSecs upper limit of the randomised retry interval. Minimum value is 1.
   * @param finalIntervalSecs retry interval before the last retry.
   * @deprecated replaced by {@link io.airbyte.api.client2.AirbyteApiClient}
   */
  @Deprecated
  @VisibleForTesting
  // This is okay since we are logging the stack trace, which PMD is not detecting.
  @SuppressWarnings("PMD.PreserveStackTrace")
  public static <T> T retryWithJitter(final Callable<T> call,
                                      final String desc,
                                      final int jitterMaxIntervalSecs,
                                      final int finalIntervalSecs,
                                      final int maxTries) {
    try {
      return retryWithJitterThrows(call, desc, jitterMaxIntervalSecs, finalIntervalSecs, maxTries);
    } catch (final Exception e) {
      LOGGER.error("retryWithJitter caught and ignoring exception:\n{}: {}", desc, e.getMessage(), e);
      // Swallowing exception on purpose
      return null;
    }
  }

  /**
   * DEPRECATED: Use {@link io.airbyte.api.client2.AirbyteApiClient} instead.
   * <p>
   * Default to 4 retries with a randomised 1 - 10 seconds interval between the first two retries and
   * an 10-minute wait for the last retry.
   *
   * @param call method to execute
   * @param desc description of what is happening
   * @param <T> type of return type
   * @return value returned by method
   * @throws Exception exception while jittering
   * @deprecated replaced by {@link io.airbyte.api.client2.AirbyteApiClient}
   */
  @Deprecated
  public static <T> T retryWithJitterThrows(final Callable<T> call, final String desc) throws Exception {
    return retryWithJitterThrows(call, desc, DEFAULT_RETRY_INTERVAL_SECS, DEFAULT_FINAL_INTERVAL_SECS, DEFAULT_MAX_RETRIES);
  }

  /**
   * DEPRECATED: Use {@link io.airbyte.api.client2.AirbyteApiClient} instead.
   * <p>
   * Provides a simple retry wrapper for api calls. This retry behaviour is slightly different from
   * generally available retries libraries - the last retry is able to wait an interval inconsistent
   * with regular intervals/exponential backoff.
   * <p>
   * Since the primary retries use case is long-running workflows, the benefit of waiting a couple of
   * minutes as a last ditch effort to outlast networking disruption outweighs the cost of slightly
   * longer jobs.
   *
   * @param call method to execute
   * @param desc short readable explanation of why this method is executed
   * @param jitterMaxIntervalSecs upper limit of the randomised retry interval. Minimum value is 1.
   * @param finalIntervalSecs retry interval before the last retry.
   * @deprecated replaced by {@link io.airbyte.api.client2.AirbyteApiClient}
   */
  @VisibleForTesting
  @Deprecated
  // This is okay since we are logging the stack trace, which PMD is not detecting.
  @SuppressWarnings("PMD.PreserveStackTrace")
  public static <T> T retryWithJitterThrows(final Callable<T> call,
                                            final String desc,
                                            final int jitterMaxIntervalSecs,
                                            final int finalIntervalSecs,
                                            final int maxTries)
      throws Exception {
    int currRetries = 0;
    boolean keepTrying = true;

    T data = null;
    while (keepTrying && currRetries < maxTries) {
      try {
        LOGGER.info("Attempt {} to {}", currRetries, desc);
        data = call.call();

        keepTrying = false;
      } catch (final Exception e) {
        LOGGER.info("Attempt {} to {} error: {}", currRetries, desc, e);
        currRetries++;

        // Sleep anywhere from 1 to jitterMaxIntervalSecs seconds.
        final var backoffTimeSecs = Math.max(RANDOM.nextInt(jitterMaxIntervalSecs + 1), 1);
        var backoffTimeMs = backoffTimeSecs * 1000;

        if (currRetries == maxTries - 1) {
          // sleep for finalIntervalMins on the last attempt.
          backoffTimeMs = finalIntervalSecs * 1000;
        }

        // We exceeded our retries, throw the last error to avoid silent failures
        if (currRetries >= maxTries) {
          throw e;
        }

        try {
          Thread.sleep(backoffTimeMs);
        } catch (final InterruptedException ex) {
          throw new RuntimeException(ex);
        }
      }
    }
    return data;
  }

}
