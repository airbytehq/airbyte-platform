/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.discover.catalog;

import static io.airbyte.config.helpers.LogClientSingleton.fullLogPath;
import static io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.ATTEMPT_NUMBER_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.DOCKER_IMAGE_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ID_KEY;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import datadog.trace.api.Trace;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.Geography;
import io.airbyte.api.client.model.generated.ScopeType;
import io.airbyte.api.client.model.generated.SecretPersistenceConfig;
import io.airbyte.api.client.model.generated.SecretPersistenceConfigGetRequestBody;
import io.airbyte.api.client.model.generated.WorkspaceIdRequestBody;
import io.airbyte.commons.converters.ConnectorConfigUpdater;
import io.airbyte.commons.features.FeatureFlags;
import io.airbyte.commons.functional.CheckedSupplier;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.protocol.AirbyteMessageSerDeProvider;
import io.airbyte.commons.protocol.AirbyteProtocolVersionedMigratorFactory;
import io.airbyte.commons.temporal.HeartbeatUtils;
import io.airbyte.commons.workers.config.WorkerConfigs;
import io.airbyte.commons.workers.config.WorkerConfigsProvider;
import io.airbyte.commons.workers.config.WorkerConfigsProvider.ResourceType;
import io.airbyte.config.ActorType;
import io.airbyte.config.Configs.WorkerEnvironment;
import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.config.FailureReason;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.config.StandardDiscoverCatalogInput;
import io.airbyte.config.helpers.LogConfigs;
import io.airbyte.config.helpers.ResourceRequirementsUtils;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.Organization;
import io.airbyte.featureflag.UseRuntimeSecretPersistence;
import io.airbyte.featureflag.UseWorkloadApiForDiscover;
import io.airbyte.featureflag.WorkloadCheckFrequencyInSeconds;
import io.airbyte.featureflag.Workspace;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricTags;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.workers.Worker;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.general.DefaultDiscoverCatalogWorker;
import io.airbyte.workers.helper.GsonPksExtractor;
import io.airbyte.workers.helper.SecretPersistenceConfigHelper;
import io.airbyte.workers.internal.AirbyteStreamFactory;
import io.airbyte.workers.internal.VersionedAirbyteStreamFactory;
import io.airbyte.workers.models.DiscoverCatalogInput;
import io.airbyte.workers.process.AirbyteIntegrationLauncher;
import io.airbyte.workers.process.IntegrationLauncher;
import io.airbyte.workers.process.Metadata;
import io.airbyte.workers.process.ProcessFactory;
import io.airbyte.workers.temporal.TemporalAttemptExecution;
import io.airbyte.workers.workload.JobOutputDocStore;
import io.airbyte.workers.workload.WorkloadIdGenerator;
import io.airbyte.workers.workload.exception.DocStoreAccessException;
import io.airbyte.workload.api.client.generated.WorkloadApi;
import io.airbyte.workload.api.client.model.generated.Workload;
import io.airbyte.workload.api.client.model.generated.WorkloadCreateRequest;
import io.airbyte.workload.api.client.model.generated.WorkloadLabel;
import io.airbyte.workload.api.client.model.generated.WorkloadStatus;
import io.airbyte.workload.api.client.model.generated.WorkloadType;
import io.micronaut.context.annotation.Value;
import io.micronaut.http.HttpStatus;
import io.temporal.activity.Activity;
import io.temporal.activity.ActivityExecutionContext;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.openapitools.client.infrastructure.ClientException;

/**
 * DiscoverCatalogActivityImpl.
 */
@Singleton
@Slf4j
public class DiscoverCatalogActivityImpl implements DiscoverCatalogActivity {

  private final WorkerConfigsProvider workerConfigsProvider;
  private final ProcessFactory processFactory;
  private final SecretsRepositoryReader secretsRepositoryReader;
  private final Path workspaceRoot;
  private final WorkerEnvironment workerEnvironment;
  private final LogConfigs logConfigs;
  private final AirbyteApiClient airbyteApiClient;
  private final String airbyteVersion;
  private final AirbyteMessageSerDeProvider serDeProvider;
  private final AirbyteProtocolVersionedMigratorFactory migratorFactory;
  private final FeatureFlags featureFlags;
  private final MetricClient metricClient;
  private final FeatureFlagClient featureFlagClient;
  private final GsonPksExtractor gsonPksExtractor;
  private final WorkloadApi workloadApi;
  private final WorkloadIdGenerator workloadIdGenerator;
  private final JobOutputDocStore jobOutputDocStore;

  public DiscoverCatalogActivityImpl(final WorkerConfigsProvider workerConfigsProvider,
                                     final ProcessFactory processFactory,
                                     final SecretsRepositoryReader secretsRepositoryReader,
                                     @Named("workspaceRoot") final Path workspaceRoot,
                                     final WorkerEnvironment workerEnvironment,
                                     final LogConfigs logConfigs,
                                     final AirbyteApiClient airbyteApiClient,
                                     @Value("${airbyte.version}") final String airbyteVersion,
                                     final AirbyteMessageSerDeProvider serDeProvider,
                                     final AirbyteProtocolVersionedMigratorFactory migratorFactory,
                                     final FeatureFlags featureFlags,
                                     final MetricClient metricClient,
                                     final FeatureFlagClient featureFlagClient,
                                     final GsonPksExtractor gsonPksExtractor,
                                     final WorkloadApi workloadApi,
                                     final WorkloadIdGenerator workloadIdGenerator,
                                     final JobOutputDocStore jobOutputDocStore) {
    this.workerConfigsProvider = workerConfigsProvider;
    this.processFactory = processFactory;
    this.secretsRepositoryReader = secretsRepositoryReader;
    this.workspaceRoot = workspaceRoot;
    this.workerEnvironment = workerEnvironment;
    this.logConfigs = logConfigs;
    this.airbyteApiClient = airbyteApiClient;
    this.airbyteVersion = airbyteVersion;
    this.serDeProvider = serDeProvider;
    this.migratorFactory = migratorFactory;
    this.featureFlags = featureFlags;
    this.metricClient = metricClient;
    this.featureFlagClient = featureFlagClient;
    this.gsonPksExtractor = gsonPksExtractor;
    this.workloadApi = workloadApi;
    this.workloadIdGenerator = workloadIdGenerator;
    this.jobOutputDocStore = jobOutputDocStore;
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public ConnectorJobOutput run(final JobRunConfig jobRunConfig,
                                final IntegrationLauncherConfig launcherConfig,
                                final StandardDiscoverCatalogInput config) {
    metricClient.count(OssMetricsRegistry.ACTIVITY_DISCOVER_CATALOG, 1);

    ApmTraceUtils.addTagsToTrace(Map.of(ATTEMPT_NUMBER_KEY, jobRunConfig.getAttemptId(), JOB_ID_KEY, jobRunConfig.getJobId(), DOCKER_IMAGE_KEY,
        launcherConfig.getDockerImage()));
    final JsonNode fullConfig;
    final UUID organizationId = config.getActorContext().getOrganizationId();
    if (organizationId != null && featureFlagClient.boolVariation(UseRuntimeSecretPersistence.INSTANCE, new Organization(organizationId))) {
      try {
        final SecretPersistenceConfig secretPersistenceConfig = airbyteApiClient.getSecretPersistenceConfigApi().getSecretsPersistenceConfig(
            new SecretPersistenceConfigGetRequestBody().scopeType(ScopeType.ORGANIZATION).scopeId(organizationId));
        final RuntimeSecretPersistence runtimeSecretPersistence =
            SecretPersistenceConfigHelper.fromApiSecretPersistenceConfig(secretPersistenceConfig);
        fullConfig = secretsRepositoryReader.hydrateConfigFromRuntimeSecretPersistence(config.getConnectionConfiguration(), runtimeSecretPersistence);
      } catch (final ApiException e) {
        throw new RuntimeException(e);
      }
    } else {
      fullConfig = secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(config.getConnectionConfiguration());
    }

    final StandardDiscoverCatalogInput input = new StandardDiscoverCatalogInput()
        .withConnectionConfiguration(fullConfig)
        .withSourceId(config.getSourceId())
        .withConnectorVersion(config.getConnectorVersion())
        .withConfigHash(config.getConfigHash());

    final ActivityExecutionContext context = Activity.getExecutionContext();
    final AtomicReference<Runnable> cancellationCallback = new AtomicReference<>(null);

    return HeartbeatUtils.withBackgroundHeartbeat(cancellationCallback,
        () -> {
          final var worker = getWorkerFactory(launcherConfig, config.getResourceRequirements()).get();
          cancellationCallback.set(worker::cancel);
          final TemporalAttemptExecution<StandardDiscoverCatalogInput, ConnectorJobOutput> temporalAttemptExecution =
              new TemporalAttemptExecution<>(
                  workspaceRoot,
                  workerEnvironment,
                  logConfigs,
                  jobRunConfig,
                  worker,
                  input,
                  airbyteApiClient,
                  airbyteVersion,
                  () -> context);
          return temporalAttemptExecution.get();
        },
        context);
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public ConnectorJobOutput runWithWorkload(final DiscoverCatalogInput input) throws WorkerException {
    final String jobId = input.getJobRunConfig().getJobId();
    final int attemptNumber = input.getJobRunConfig().getAttemptId() == null ? 0 : Math.toIntExact(input.getJobRunConfig().getAttemptId());
    final String workloadId =
        workloadIdGenerator.generateCheckWorkloadId(input.getDiscoverCatalogInput().getActorContext().getActorDefinitionId(), jobId,
            attemptNumber);
    final String serializedInput = Jsons.serialize(input);

    final UUID workspaceId = input.getDiscoverCatalogInput().getActorContext().getWorkspaceId();
    final Geography geo = getGeography(Optional.ofNullable(input.getLauncherConfig().getConnectionId()),
        Optional.ofNullable(workspaceId));

    final WorkloadCreateRequest workloadCreateRequest = new WorkloadCreateRequest(
        workloadId,
        List.of(new WorkloadLabel(Metadata.JOB_LABEL_KEY, jobId.toString()),
            new WorkloadLabel(Metadata.ATTEMPT_LABEL_KEY, String.valueOf(attemptNumber)),
            new WorkloadLabel(Metadata.WORKSPACE_LABEL_KEY, workspaceId.toString()),
            new WorkloadLabel(Metadata.ACTOR_TYPE, String.valueOf(ActorType.SOURCE.toString()))),
        serializedInput,
        fullLogPath(Path.of(workloadId)),
        geo.getValue(),
        WorkloadType.DISCOVER,
        null,
        null);

    createWorkload(workloadCreateRequest);

    try {
      Workload workload = workloadApi.workloadGet(workloadId);
      final int checkFrequencyInSeconds = featureFlagClient.intVariation(WorkloadCheckFrequencyInSeconds.INSTANCE,
          new Workspace(workspaceId));
      while (!isWorkloadTerminal(workload)) {
        Thread.sleep(1000 * checkFrequencyInSeconds);
        workload = workloadApi.workloadGet(workloadId);
      }
    } catch (final IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }

    try {
      final Optional<ConnectorJobOutput> connectorJobOutput = jobOutputDocStore.read(workloadId);

      log.error(String.valueOf(connectorJobOutput.get()));
      return connectorJobOutput.orElse(new ConnectorJobOutput()
          .withOutputType(ConnectorJobOutput.OutputType.DISCOVER_CATALOG_ID)
          .withDiscoverCatalogId(null)
          .withFailureReason(new FailureReason()
              .withFailureType(FailureReason.FailureType.CONFIG_ERROR)
              .withInternalMessage("Unable to read output for workload: " + workloadId)
              .withExternalMessage("Unable to read output")));
    } catch (final DocStoreAccessException e) {
      throw new WorkerException("Unable to read output", e);
    }
  }

  @Override
  public boolean shouldUseWorkload(final UUID workspaceId) {
    return featureFlagClient.boolVariation(UseWorkloadApiForDiscover.INSTANCE, new Workspace(workspaceId));
  }

  @Override
  public void reportSuccess() {
    metricClient.count(OssMetricsRegistry.CATALOG_DISCOVERY, 1, new MetricAttribute(MetricTags.STATUS, "success"));
  }

  @Override
  public void reportFailure() {
    metricClient.count(OssMetricsRegistry.CATALOG_DISCOVERY, 1, new MetricAttribute(MetricTags.STATUS, "failed"));
  }

  @VisibleForTesting
  boolean isWorkloadTerminal(final Workload workload) {
    final var status = workload.getStatus();
    return status == WorkloadStatus.SUCCESS || status == WorkloadStatus.FAILURE || status == WorkloadStatus.CANCELLED;
  }

  @VisibleForTesting
  Geography getGeography(final Optional<UUID> maybeConnectionId, final Optional<UUID> maybeWorkspaceId) throws WorkerException {
    try {
      return maybeConnectionId
          .map(connectionId -> {
            try {
              return airbyteApiClient.getConnectionApi().getConnection(new ConnectionIdRequestBody().connectionId(connectionId)).getGeography();
            } catch (final ApiException e) {
              throw new RuntimeException(e);
            }
          }).orElse(
              maybeWorkspaceId.map(
                  workspaceId -> {
                    try {
                      return airbyteApiClient.getWorkspaceApi().getWorkspace(new WorkspaceIdRequestBody().workspaceId(workspaceId))
                          .getDefaultGeography();
                    } catch (final ApiException e) {
                      throw new RuntimeException(e);
                    }
                  })
                  .orElse(Geography.AUTO));
    } catch (final Exception e) {
      throw new WorkerException("Unable to find geography of connection " + maybeConnectionId, e);
    }
  }

  private void createWorkload(final WorkloadCreateRequest workloadCreateRequest) {
    try {
      workloadApi.workloadCreate(workloadCreateRequest);
    } catch (final ClientException e) {
      /*
       * The Workload API returns a 409 response when the request to execute the workload has already been
       * created. That response is handled in the form of a ClientException by the generated OpenAPI
       * client. We do not want to cause the Temporal workflow to retry, so catch it and log the
       * information so that the workflow will continue.
       */
      if (e.getStatusCode() == HttpStatus.CONFLICT.getCode()) {
        log.warn("Workload {} already created and in progress.  Continuing...", workloadCreateRequest.getWorkloadId());
      } else {
        throw new RuntimeException(e);
      }
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("LineLength")
  private CheckedSupplier<Worker<StandardDiscoverCatalogInput, ConnectorJobOutput>, Exception> getWorkerFactory(
                                                                                                                final IntegrationLauncherConfig launcherConfig,
                                                                                                                final ResourceRequirements actorDefinitionResourceRequirements) {
    return () -> {
      final WorkerConfigs workerConfigs = workerConfigsProvider.getConfig(ResourceType.DISCOVER);
      final ResourceRequirements defaultWorkerConfigResourceRequirements = workerConfigs.getResourceRequirements();

      final IntegrationLauncher integrationLauncher =
          new AirbyteIntegrationLauncher(launcherConfig.getJobId(), launcherConfig.getAttemptId().intValue(), launcherConfig.getConnectionId(),
              launcherConfig.getWorkspaceId(), launcherConfig.getDockerImage(),
              processFactory,
              ResourceRequirementsUtils.getResourceRequirements(actorDefinitionResourceRequirements, defaultWorkerConfigResourceRequirements),
              null,
              launcherConfig.getAllowedHosts(), launcherConfig.getIsCustomConnector(),
              featureFlags, Collections.emptyMap(), Collections.emptyMap());
      final AirbyteStreamFactory streamFactory =
          new VersionedAirbyteStreamFactory<>(serDeProvider, migratorFactory, launcherConfig.getProtocolVersion(), Optional.empty(), Optional.empty(),
              Optional.empty(), new VersionedAirbyteStreamFactory.InvalidLineFailureConfiguration(false, false, false), gsonPksExtractor);
      final ConnectorConfigUpdater connectorConfigUpdater =
          new ConnectorConfigUpdater(airbyteApiClient.getSourceApi(), airbyteApiClient.getDestinationApi());
      return new DefaultDiscoverCatalogWorker(airbyteApiClient, integrationLauncher, connectorConfigUpdater, streamFactory);
    };
  }

}
