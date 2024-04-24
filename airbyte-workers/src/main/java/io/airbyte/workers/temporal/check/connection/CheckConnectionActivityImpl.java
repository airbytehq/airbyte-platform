/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.check.connection;

import static io.airbyte.config.helpers.LogClientSingleton.fullLogPath;
import static io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.ATTEMPT_NUMBER_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.DOCKER_IMAGE_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ID_KEY;

import com.google.common.annotations.VisibleForTesting;
import datadog.trace.api.Trace;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.Geography;
import io.airbyte.api.client.model.generated.WorkspaceIdRequestBody;
import io.airbyte.commons.converters.ConnectorConfigUpdater;
import io.airbyte.commons.features.FeatureFlags;
import io.airbyte.commons.functional.CheckedSupplier;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.protocol.AirbyteMessageSerDeProvider;
import io.airbyte.commons.protocol.AirbyteProtocolVersionedMigratorFactory;
import io.airbyte.commons.temporal.HeartbeatUtils;
import io.airbyte.commons.version.AirbyteProtocolVersion;
import io.airbyte.commons.workers.config.WorkerConfigs;
import io.airbyte.commons.workers.config.WorkerConfigsProvider;
import io.airbyte.commons.workers.config.WorkerConfigsProvider.ResourceType;
import io.airbyte.config.Configs.WorkerEnvironment;
import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.config.ConnectorJobOutput.OutputType;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.config.StandardCheckConnectionInput;
import io.airbyte.config.StandardCheckConnectionOutput;
import io.airbyte.config.StandardCheckConnectionOutput.Status;
import io.airbyte.config.helpers.LogConfigs;
import io.airbyte.config.helpers.ResourceRequirementsUtils;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.UseWorkloadApiForCheck;
import io.airbyte.featureflag.WorkloadCheckFrequencyInSeconds;
import io.airbyte.featureflag.Workspace;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricClientFactory;
import io.airbyte.metrics.lib.MetricTags;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.workers.CheckConnectionInputHydrator;
import io.airbyte.workers.ConnectorSecretsHydrator;
import io.airbyte.workers.Worker;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.general.DefaultCheckConnectionWorker;
import io.airbyte.workers.helper.GsonPksExtractor;
import io.airbyte.workers.internal.AirbyteStreamFactory;
import io.airbyte.workers.internal.VersionedAirbyteStreamFactory;
import io.airbyte.workers.models.CheckConnectionInput;
import io.airbyte.workers.process.AirbyteIntegrationLauncher;
import io.airbyte.workers.process.IntegrationLauncher;
import io.airbyte.workers.process.Metadata;
import io.airbyte.workers.process.ProcessFactory;
import io.airbyte.workers.sync.WorkloadClient;
import io.airbyte.workers.temporal.TemporalAttemptExecution;
import io.airbyte.workers.workload.WorkloadIdGenerator;
import io.airbyte.workload.api.client.model.generated.WorkloadCreateRequest;
import io.airbyte.workload.api.client.model.generated.WorkloadLabel;
import io.airbyte.workload.api.client.model.generated.WorkloadPriority;
import io.airbyte.workload.api.client.model.generated.WorkloadType;
import io.micronaut.context.annotation.Value;
import io.temporal.activity.Activity;
import io.temporal.activity.ActivityExecutionContext;
import io.temporal.activity.ActivityOptions;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

/**
 * Check connection activity temporal implementation for the control plane.
 */
@Singleton
@Slf4j
public class CheckConnectionActivityImpl implements CheckConnectionActivity {

  private final WorkerConfigsProvider workerConfigsProvider;
  private final ProcessFactory processFactory;
  private final Path workspaceRoot;
  private final WorkerEnvironment workerEnvironment;
  private final LogConfigs logConfigs;
  private final AirbyteApiClient airbyteApiClient;
  private final String airbyteVersion;
  private final AirbyteMessageSerDeProvider serDeProvider;
  private final AirbyteProtocolVersionedMigratorFactory migratorFactory;
  private final FeatureFlags featureFlags;
  private final GsonPksExtractor gsonPksExtractor;
  private final CheckConnectionInputHydrator inputHydrator;
  private final WorkloadClient workloadClient;
  private final WorkloadIdGenerator workloadIdGenerator;
  private final FeatureFlagClient featureFlagClient;
  private final MetricClient metricClient;
  private final ActivityOptions activityOptions;

  public CheckConnectionActivityImpl(final WorkerConfigsProvider workerConfigsProvider,
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
                                     final FeatureFlagClient featureFlagClient,
                                     final GsonPksExtractor gsonPksExtractor,
                                     final WorkloadClient workloadClient,
                                     final WorkloadIdGenerator workloadIdGenerator,
                                     final MetricClient metricClient,
                                     @Named("checkActivityOptions") final ActivityOptions activityOptions) {
    this(workerConfigsProvider,
        processFactory,
        secretsRepositoryReader,
        workspaceRoot,
        workerEnvironment,
        logConfigs,
        airbyteApiClient,
        airbyteVersion,
        serDeProvider,
        migratorFactory,
        featureFlags,
        featureFlagClient,
        gsonPksExtractor,
        workloadClient,
        workloadIdGenerator,
        new CheckConnectionInputHydrator(
            new ConnectorSecretsHydrator(
                secretsRepositoryReader,
                airbyteApiClient.getSecretPersistenceConfigApi(),
                featureFlagClient)),
        metricClient,
        activityOptions);
  }

  @VisibleForTesting
  CheckConnectionActivityImpl(final WorkerConfigsProvider workerConfigsProvider,
                              final ProcessFactory processFactory,
                              final SecretsRepositoryReader secretsRepositoryReader,
                              final Path workspaceRoot,
                              final WorkerEnvironment workerEnvironment,
                              final LogConfigs logConfigs,
                              final AirbyteApiClient airbyteApiClient,
                              final String airbyteVersion,
                              final AirbyteMessageSerDeProvider serDeProvider,
                              final AirbyteProtocolVersionedMigratorFactory migratorFactory,
                              final FeatureFlags featureFlags,
                              final FeatureFlagClient featureFlagClient,
                              final GsonPksExtractor gsonPksExtractor,
                              final WorkloadClient workloadClient,
                              final WorkloadIdGenerator workloadIdGenerator,
                              final CheckConnectionInputHydrator checkConnectionInputHydrator,
                              final MetricClient metricClient,
                              final ActivityOptions activityOptions) {
    this.workerConfigsProvider = workerConfigsProvider;
    this.processFactory = processFactory;
    this.workspaceRoot = workspaceRoot;
    this.workerEnvironment = workerEnvironment;
    this.logConfigs = logConfigs;
    this.airbyteApiClient = airbyteApiClient;
    this.airbyteVersion = airbyteVersion;
    this.serDeProvider = serDeProvider;
    this.migratorFactory = migratorFactory;
    this.featureFlags = featureFlags;
    this.gsonPksExtractor = gsonPksExtractor;
    this.workloadClient = workloadClient;
    this.workloadIdGenerator = workloadIdGenerator;
    this.featureFlagClient = featureFlagClient;
    this.inputHydrator = checkConnectionInputHydrator;
    this.metricClient = metricClient;
    this.activityOptions = activityOptions;
  }

  @Override
  public Duration getCheckConnectionTimeout() {
    return activityOptions.getStartToCloseTimeout();
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public ConnectorJobOutput runWithJobOutput(final CheckConnectionInput args) {
    MetricClientFactory.getMetricClient().count(OssMetricsRegistry.ACTIVITY_CHECK_CONNECTION, 1);

    ApmTraceUtils
        .addTagsToTrace(Map.of(ATTEMPT_NUMBER_KEY, args.getJobRunConfig().getAttemptId(), JOB_ID_KEY, args.getJobRunConfig().getJobId(),
            DOCKER_IMAGE_KEY, args.getLauncherConfig().getDockerImage()));
    final StandardCheckConnectionInput rawInput = args.getCheckConnectionInput();
    final StandardCheckConnectionInput input = inputHydrator.getHydratedStandardCheckInput(rawInput);

    final ActivityExecutionContext context = Activity.getExecutionContext();
    final AtomicReference<Runnable> cancellationCallback = new AtomicReference<>(null);

    return HeartbeatUtils.withBackgroundHeartbeat(cancellationCallback,
        () -> {
          final var worker = getWorkerFactory(args.getLauncherConfig(), rawInput.getResourceRequirements()).get();
          cancellationCallback.set(worker::cancel);
          final TemporalAttemptExecution<StandardCheckConnectionInput, ConnectorJobOutput> temporalAttemptExecution =
              new TemporalAttemptExecution<>(
                  workspaceRoot,
                  workerEnvironment,
                  logConfigs,
                  args.getJobRunConfig(),
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
  public ConnectorJobOutput runWithWorkload(final CheckConnectionInput input) throws WorkerException {
    final String jobId = input.getJobRunConfig().getJobId();
    final int attemptNumber = input.getJobRunConfig().getAttemptId() == null ? 0 : Math.toIntExact(input.getJobRunConfig().getAttemptId());
    final String workloadId =
        workloadIdGenerator.generateCheckWorkloadId(input.getCheckConnectionInput().getActorContext().getActorDefinitionId(), jobId,
            attemptNumber);
    final String serializedInput = Jsons.serialize(input);

    final UUID workspaceId = input.getCheckConnectionInput().getActorContext().getWorkspaceId();
    final Geography geo = getGeography(Optional.ofNullable(input.getLauncherConfig().getConnectionId()),
        Optional.ofNullable(workspaceId));

    final WorkloadCreateRequest workloadCreateRequest = new WorkloadCreateRequest(
        workloadId,
        List.of(new WorkloadLabel(Metadata.JOB_LABEL_KEY, jobId.toString()),
            new WorkloadLabel(Metadata.ATTEMPT_LABEL_KEY, String.valueOf(attemptNumber)),
            new WorkloadLabel(Metadata.WORKSPACE_LABEL_KEY, workspaceId.toString()),
            new WorkloadLabel(Metadata.ACTOR_TYPE, String.valueOf(input.getCheckConnectionInput().getActorType().toString()))),
        serializedInput,
        fullLogPath(Path.of(workloadId)),
        geo.getValue(),
        WorkloadType.CHECK,
        WorkloadPriority.Companion.decode(input.getLauncherConfig().getPriority().toString()),
        null,
        null);

    workloadClient.createWorkload(workloadCreateRequest);

    final int checkFrequencyInSeconds =
        featureFlagClient.intVariation(WorkloadCheckFrequencyInSeconds.INSTANCE, new Workspace(workspaceId));
    workloadClient.waitForWorkload(workloadId, checkFrequencyInSeconds);

    final var output = workloadClient.getConnectorJobOutput(
        workloadId,
        failureReason -> new ConnectorJobOutput()
            .withOutputType(OutputType.CHECK_CONNECTION)
            .withCheckConnection(
                new StandardCheckConnectionOutput()
                    .withStatus(Status.FAILED)
                    .withMessage(failureReason.getExternalMessage()))
            .withFailureReason(failureReason));

    metricClient.count(
        OssMetricsRegistry.SIDECAR_CHECK,
        1,
        new MetricAttribute(MetricTags.STATUS, output.getCheckConnection().getStatus() == Status.FAILED ? "failed" : "success"));
    return output;
  }

  @Override
  public boolean shouldUseWorkload(final UUID workspaceId) {
    return featureFlagClient.boolVariation(UseWorkloadApiForCheck.INSTANCE, new Workspace(workspaceId));
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

  @SuppressWarnings("LineLength")
  private CheckedSupplier<Worker<StandardCheckConnectionInput, ConnectorJobOutput>, Exception> getWorkerFactory(
                                                                                                                final IntegrationLauncherConfig launcherConfig,
                                                                                                                final ResourceRequirements actorDefinitionResourceRequirements) {
    return () -> {
      final WorkerConfigs workerConfigs = workerConfigsProvider.getConfig(ResourceType.CHECK);
      final ResourceRequirements defaultWorkerConfigResourceRequirements = workerConfigs.getResourceRequirements();

      final IntegrationLauncher integrationLauncher = new AirbyteIntegrationLauncher(
          launcherConfig.getJobId(),
          Math.toIntExact(launcherConfig.getAttemptId()),
          launcherConfig.getConnectionId(),
          launcherConfig.getWorkspaceId(),
          launcherConfig.getDockerImage(),
          processFactory,
          ResourceRequirementsUtils.getResourceRequirements(actorDefinitionResourceRequirements, defaultWorkerConfigResourceRequirements),
          null,
          launcherConfig.getAllowedHosts(),
          launcherConfig.getIsCustomConnector(),
          featureFlags,
          Collections.emptyMap(),
          Collections.emptyMap());

      final ConnectorConfigUpdater connectorConfigUpdater = new ConnectorConfigUpdater(
          airbyteApiClient.getSourceApi(),
          airbyteApiClient.getDestinationApi());

      final var protocolVersion =
          launcherConfig.getProtocolVersion() != null ? launcherConfig.getProtocolVersion() : AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION;
      final AirbyteStreamFactory streamFactory =
          new VersionedAirbyteStreamFactory<>(serDeProvider, migratorFactory, protocolVersion, Optional.empty(), Optional.empty(),
              new VersionedAirbyteStreamFactory.InvalidLineFailureConfiguration(false),
              gsonPksExtractor);

      return new DefaultCheckConnectionWorker(integrationLauncher, connectorConfigUpdater, streamFactory);
    };
  }

}
