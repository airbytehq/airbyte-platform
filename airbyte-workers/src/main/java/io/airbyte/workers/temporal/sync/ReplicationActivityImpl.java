/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync;

import static io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.ATTEMPT_NUMBER_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.CONNECTION_ID_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.DESTINATION_DOCKER_IMAGE_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ID_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.REPLICATION_BYTES_SYNCED_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.REPLICATION_RECORDS_SYNCED_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.REPLICATION_STATUS_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.SOURCE_DOCKER_IMAGE_KEY;

import datadog.trace.api.Trace;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.model.generated.SourceDefinitionIdRequestBody;
import io.airbyte.api.client.model.generated.SourceIdRequestBody;
import io.airbyte.commons.converters.ConnectorConfigUpdater;
import io.airbyte.commons.features.FeatureFlagHelper;
import io.airbyte.commons.features.FeatureFlags;
import io.airbyte.commons.functional.CheckedSupplier;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.protocol.AirbyteMessageSerDeProvider;
import io.airbyte.commons.protocol.AirbyteProtocolVersionedMigratorFactory;
import io.airbyte.commons.temporal.CancellationHandler;
import io.airbyte.commons.temporal.TemporalUtils;
import io.airbyte.config.AirbyteConfigValidator;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.Configs.WorkerEnvironment;
import io.airbyte.config.ReplicationAttemptSummary;
import io.airbyte.config.ReplicationOutput;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.config.StandardSyncInput;
import io.airbyte.config.StandardSyncOutput;
import io.airbyte.config.StandardSyncSummary;
import io.airbyte.config.helpers.LogConfigs;
import io.airbyte.config.persistence.split_secrets.SecretsHydrator;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.FieldSelectionEnabled;
import io.airbyte.featureflag.PerfBackgroundJsonValidation;
import io.airbyte.featureflag.ShouldStartHeartbeatMonitoring;
import io.airbyte.featureflag.Workspace;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricClientFactory;
import io.airbyte.metrics.lib.MetricEmittingApps;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.workers.ContainerOrchestratorConfig;
import io.airbyte.workers.RecordSchemaValidator;
import io.airbyte.workers.Worker;
import io.airbyte.workers.WorkerConfigs;
import io.airbyte.workers.WorkerConstants;
import io.airbyte.workers.WorkerMetricReporter;
import io.airbyte.workers.WorkerUtils;
import io.airbyte.workers.general.DefaultReplicationWorker;
import io.airbyte.workers.internal.AirbyteSource;
import io.airbyte.workers.internal.DefaultAirbyteDestination;
import io.airbyte.workers.internal.DefaultAirbyteSource;
import io.airbyte.workers.internal.EmptyAirbyteSource;
import io.airbyte.workers.internal.HeartbeatMonitor;
import io.airbyte.workers.internal.HeartbeatTimeoutChaperone;
import io.airbyte.workers.internal.NamespacingMapper;
import io.airbyte.workers.internal.VersionedAirbyteMessageBufferedWriterFactory;
import io.airbyte.workers.internal.VersionedAirbyteStreamFactory;
import io.airbyte.workers.internal.book_keeping.AirbyteMessageTracker;
import io.airbyte.workers.internal.exception.DestinationException;
import io.airbyte.workers.internal.exception.SourceException;
import io.airbyte.workers.internal.sync_persistence.SyncPersistenceFactory;
import io.airbyte.workers.process.AirbyteIntegrationLauncher;
import io.airbyte.workers.process.IntegrationLauncher;
import io.airbyte.workers.process.ProcessFactory;
import io.airbyte.workers.sync.ReplicationLauncherWorker;
import io.airbyte.workers.temporal.TemporalAttemptExecution;
import io.micronaut.context.annotation.Value;
import io.temporal.activity.Activity;
import io.temporal.activity.ActivityExecutionContext;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Replication temporal activity impl.
 */
@Singleton
public class ReplicationActivityImpl implements ReplicationActivity {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationActivityImpl.class);
  private static final int MAX_TEMPORAL_MESSAGE_SIZE = 2 * 1024 * 1024;

  private final Optional<ContainerOrchestratorConfig> containerOrchestratorConfig;
  private final ProcessFactory processFactory;
  private final SecretsHydrator secretsHydrator;
  private final Path workspaceRoot;
  private final WorkerEnvironment workerEnvironment;
  private final LogConfigs logConfigs;
  private final String airbyteVersion;
  private final FeatureFlags featureFlags;
  private final FeatureFlagClient featureFlagClient;
  private final Integer serverPort;
  private final AirbyteConfigValidator airbyteConfigValidator;
  private final TemporalUtils temporalUtils;
  private final AirbyteApiClient airbyteApiClient;
  private final AirbyteMessageSerDeProvider serDeProvider;
  private final AirbyteProtocolVersionedMigratorFactory migratorFactory;
  private final WorkerConfigs workerConfigs;
  private final SyncPersistenceFactory syncPersistenceFactory;

  public ReplicationActivityImpl(@Named("containerOrchestratorConfig") final Optional<ContainerOrchestratorConfig> containerOrchestratorConfig,
                                 @Named("replicationProcessFactory") final ProcessFactory processFactory,
                                 final SecretsHydrator secretsHydrator,
                                 @Named("workspaceRoot") final Path workspaceRoot,
                                 final WorkerEnvironment workerEnvironment,
                                 final LogConfigs logConfigs,
                                 @Value("${airbyte.version}") final String airbyteVersion,
                                 final FeatureFlags featureFlags,
                                 @Value("${micronaut.server.port}") final Integer serverPort,
                                 final AirbyteConfigValidator airbyteConfigValidator,
                                 final TemporalUtils temporalUtils,
                                 final AirbyteApiClient airbyteApiClient,
                                 final AirbyteMessageSerDeProvider serDeProvider,
                                 final AirbyteProtocolVersionedMigratorFactory migratorFactory,
                                 @Named("replicationWorkerConfigs") final WorkerConfigs workerConfigs,
                                 final FeatureFlagClient featureFlagClient,
                                 final SyncPersistenceFactory syncPersistenceFactory) {
    this.containerOrchestratorConfig = containerOrchestratorConfig;
    this.processFactory = processFactory;
    this.secretsHydrator = secretsHydrator;
    this.workspaceRoot = workspaceRoot;
    this.workerEnvironment = workerEnvironment;
    this.logConfigs = logConfigs;
    this.airbyteVersion = airbyteVersion;
    this.featureFlags = featureFlags;
    this.serverPort = serverPort;
    this.airbyteConfigValidator = airbyteConfigValidator;
    this.temporalUtils = temporalUtils;
    this.airbyteApiClient = airbyteApiClient;
    this.serDeProvider = serDeProvider;
    this.migratorFactory = migratorFactory;
    this.workerConfigs = workerConfigs;
    this.featureFlagClient = featureFlagClient;
    this.syncPersistenceFactory = syncPersistenceFactory;
  }

  // Marking task queue as nullable because we changed activity signature; thus runs started before
  // this new change will have taskQueue set to null. We should remove it after the old runs are all
  // finished in next release.
  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public StandardSyncOutput replicate(final JobRunConfig jobRunConfig,
                                      final IntegrationLauncherConfig sourceLauncherConfig,
                                      final IntegrationLauncherConfig destinationLauncherConfig,
                                      final StandardSyncInput syncInput,
                                      @Nullable final String taskQueue) {
    final Map<String, Object> traceAttributes =
        Map.of(
            ATTEMPT_NUMBER_KEY, jobRunConfig.getAttemptId(),
            CONNECTION_ID_KEY, syncInput.getConnectionId(),
            JOB_ID_KEY, jobRunConfig.getJobId(),
            DESTINATION_DOCKER_IMAGE_KEY, destinationLauncherConfig.getDockerImage(),
            SOURCE_DOCKER_IMAGE_KEY, sourceLauncherConfig.getDockerImage());
    ApmTraceUtils
        .addTagsToTrace(traceAttributes);
    if (isResetJob(sourceLauncherConfig.getDockerImage())) {
      MetricClientFactory.getMetricClient().count(OssMetricsRegistry.RESET_REQUEST, 1);
    }
    final ActivityExecutionContext context = Activity.getExecutionContext();
    return temporalUtils.withBackgroundHeartbeat(
        () -> {

          final var fullSourceConfig = secretsHydrator.hydrate(syncInput.getSourceConfiguration());
          final var fullDestinationConfig = secretsHydrator.hydrate(syncInput.getDestinationConfiguration());

          final var fullSyncInput = Jsons.clone(syncInput)
              .withSourceConfiguration(fullSourceConfig)
              .withDestinationConfiguration(fullDestinationConfig);

          final Supplier<StandardSyncInput> inputSupplier = () -> {
            airbyteConfigValidator.ensureAsRuntime(ConfigSchema.STANDARD_SYNC_INPUT, Jsons.jsonNode(fullSyncInput));
            return fullSyncInput;
          };

          final CheckedSupplier<Worker<StandardSyncInput, ReplicationOutput>, Exception> workerFactory;

          final UUID sourceDefinitionId = AirbyteApiClient.retryWithJitter(
              () -> airbyteApiClient.getSourceApi().getSource(new SourceIdRequestBody().sourceId(syncInput.getSourceId())).getSourceDefinitionId(),
              "get source");

          final long maxSecondsBetweenMessages = AirbyteApiClient.retryWithJitter(() -> airbyteApiClient.getSourceDefinitionApi()
              .getSourceDefinition(new SourceDefinitionIdRequestBody().sourceDefinitionId(sourceDefinitionId))
              .getMaxSecondsBetweenMessages(), "get source definition");

          final HeartbeatMonitor heartbeatMonitor = new HeartbeatMonitor(Duration.ofSeconds(maxSecondsBetweenMessages));

          final HeartbeatTimeoutChaperone heartbeatTimeoutChaperone = new HeartbeatTimeoutChaperone(heartbeatMonitor,
              HeartbeatTimeoutChaperone.DEFAULT_TIMEOUT_CHECK_DURATION,
              featureFlagClient,
              syncInput.getWorkspaceId(),
              syncInput.getConnectionId(),
              MetricClientFactory.getMetricClient());

          if (containerOrchestratorConfig.isPresent()) {
            workerFactory = getContainerLauncherWorkerFactory(
                containerOrchestratorConfig.get(),
                sourceLauncherConfig,
                destinationLauncherConfig,
                jobRunConfig,
                syncInput.getResourceRequirements(),
                () -> context,
                workerConfigs,
                syncInput.getConnectionId());
          } else {
            workerFactory =
                getLegacyWorkerFactory(sourceLauncherConfig, destinationLauncherConfig, jobRunConfig, syncInput, syncPersistenceFactory,
                    heartbeatMonitor, heartbeatTimeoutChaperone);
          }

          final TemporalAttemptExecution<StandardSyncInput, ReplicationOutput> temporalAttempt =
              new TemporalAttemptExecution<>(
                  workspaceRoot,
                  workerEnvironment,
                  logConfigs,
                  jobRunConfig,
                  workerFactory,
                  inputSupplier,
                  new CancellationHandler.TemporalCancellationHandler(context),
                  airbyteApiClient,
                  airbyteVersion,
                  () -> context,
                  Optional.ofNullable(taskQueue),
                  () -> {
                    try {
                      heartbeatTimeoutChaperone.close();
                    } catch (Exception e) {
                      throw new RuntimeException(e);
                    }
                  });

          final ReplicationOutput attemptOutput = temporalAttempt.get();
          final StandardSyncOutput standardSyncOutput = reduceReplicationOutput(attemptOutput, traceAttributes);
          standardSyncOutput.setCommitStateAsap(syncInput.getCommitStateAsap());

          final String standardSyncOutputString = standardSyncOutput.toString();
          LOGGER.info("sync summary: {}", standardSyncOutputString);
          if (standardSyncOutputString.length() > MAX_TEMPORAL_MESSAGE_SIZE) {
            LOGGER.error("Sync output exceeds the max temporal message size of {}, actual is {}.", MAX_TEMPORAL_MESSAGE_SIZE,
                standardSyncOutputString.length());
          } else {
            LOGGER.info("Sync summary length: {}", standardSyncOutputString.length());
          }

          return standardSyncOutput;
        },
        () -> context);
  }

  private static StandardSyncOutput reduceReplicationOutput(final ReplicationOutput output, final Map<String, Object> metricAttributes) {
    final StandardSyncOutput standardSyncOutput = new StandardSyncOutput();
    final StandardSyncSummary syncSummary = new StandardSyncSummary();
    final ReplicationAttemptSummary replicationSummary = output.getReplicationAttemptSummary();

    traceReplicationSummary(replicationSummary, metricAttributes);

    syncSummary.setBytesSynced(replicationSummary.getBytesSynced());
    syncSummary.setRecordsSynced(replicationSummary.getRecordsSynced());
    syncSummary.setStartTime(replicationSummary.getStartTime());
    syncSummary.setEndTime(replicationSummary.getEndTime());
    syncSummary.setStatus(replicationSummary.getStatus());
    syncSummary.setTotalStats(replicationSummary.getTotalStats());
    syncSummary.setStreamStats(replicationSummary.getStreamStats());

    standardSyncOutput.setState(output.getState());
    standardSyncOutput.setOutputCatalog(output.getOutputCatalog());
    standardSyncOutput.setStandardSyncSummary(syncSummary);
    standardSyncOutput.setFailures(output.getFailures());

    return standardSyncOutput;
  }

  private static void traceReplicationSummary(final ReplicationAttemptSummary replicationSummary, final Map<String, Object> metricAttributes) {
    if (replicationSummary == null) {
      return;
    }

    final MetricAttribute[] attributes = metricAttributes.entrySet().stream()
        .map(e -> new MetricAttribute(ApmTraceUtils.formatTag(e.getKey()), e.getValue().toString()))
        .collect(Collectors.toSet()).toArray(new MetricAttribute[] {});
    final Map<String, Object> tags = new HashMap<>();
    if (replicationSummary.getBytesSynced() != null) {
      tags.put(REPLICATION_BYTES_SYNCED_KEY, replicationSummary.getBytesSynced());
      MetricClientFactory.getMetricClient().count(OssMetricsRegistry.REPLICATION_BYTES_SYNCED, replicationSummary.getBytesSynced(), attributes);
    }
    if (replicationSummary.getRecordsSynced() != null) {
      tags.put(REPLICATION_RECORDS_SYNCED_KEY, replicationSummary.getRecordsSynced());
      MetricClientFactory.getMetricClient().count(OssMetricsRegistry.REPLICATION_RECORDS_SYNCED, replicationSummary.getRecordsSynced(), attributes);
    }
    if (replicationSummary.getStatus() != null) {
      tags.put(REPLICATION_STATUS_KEY, replicationSummary.getStatus().value());
    }
    if (!tags.isEmpty()) {
      ApmTraceUtils.addTagsToTrace(tags);
    }
  }

  @SuppressWarnings("LineLength")
  private CheckedSupplier<Worker<StandardSyncInput, ReplicationOutput>, Exception> getLegacyWorkerFactory(
                                                                                                          final IntegrationLauncherConfig sourceLauncherConfig,
                                                                                                          final IntegrationLauncherConfig destinationLauncherConfig,
                                                                                                          final JobRunConfig jobRunConfig,
                                                                                                          final StandardSyncInput syncInput,
                                                                                                          final SyncPersistenceFactory syncPersistenceFactory,
                                                                                                          final HeartbeatMonitor heartbeatMonitor,
                                                                                                          final HeartbeatTimeoutChaperone heartbeatTimeoutChaperone) {
    return () -> {
      final IntegrationLauncher sourceLauncher = new AirbyteIntegrationLauncher(
          sourceLauncherConfig.getJobId(),
          Math.toIntExact(sourceLauncherConfig.getAttemptId()),
          sourceLauncherConfig.getDockerImage(),
          processFactory,
          syncInput.getSourceResourceRequirements(),
          sourceLauncherConfig.getAllowedHosts(),
          sourceLauncherConfig.getIsCustomConnector(),
          featureFlags);
      final IntegrationLauncher destinationLauncher = new AirbyteIntegrationLauncher(
          destinationLauncherConfig.getJobId(),
          Math.toIntExact(destinationLauncherConfig.getAttemptId()),
          destinationLauncherConfig.getDockerImage(),
          processFactory,
          syncInput.getDestinationResourceRequirements(),
          destinationLauncherConfig.getAllowedHosts(),
          destinationLauncherConfig.getIsCustomConnector(),
          featureFlags);

      // reset jobs use an empty source to induce resetting all data in destination.
      final AirbyteSource airbyteSource = isResetJob(sourceLauncherConfig.getDockerImage())
          ? new EmptyAirbyteSource(featureFlags.useStreamCapableState())
          : new DefaultAirbyteSource(sourceLauncher,
              new VersionedAirbyteStreamFactory<>(serDeProvider, migratorFactory, sourceLauncherConfig.getProtocolVersion(),
                  Optional.of(syncInput.getCatalog()), DefaultAirbyteSource.CONTAINER_LOG_MDC_BUILDER, Optional.of(SourceException.class)),
              heartbeatMonitor,
              migratorFactory.getProtocolSerializer(sourceLauncherConfig.getProtocolVersion()),
              featureFlags);
      MetricClientFactory.initialize(MetricEmittingApps.WORKER);
      final MetricClient metricClient = MetricClientFactory.getMetricClient();
      final WorkerMetricReporter metricReporter = new WorkerMetricReporter(metricClient, sourceLauncherConfig.getDockerImage());

      final UUID workspaceId = syncInput.getWorkspaceId();
      // NOTE: we apply field selection if the feature flag client says so (recommended) or the old
      // environment-variable flags say so (deprecated).
      // The latter FeatureFlagHelper will be removed once the flag client is fully deployed.
      final boolean fieldSelectionEnabled = workspaceId != null
          && (featureFlagClient.enabled(FieldSelectionEnabled.INSTANCE, new Workspace(workspaceId))
              || FeatureFlagHelper.isFieldSelectionEnabledForWorkspace(featureFlags, workspaceId));
      final boolean heartbeatTimeoutEnabled = workspaceId != null
          && featureFlagClient.enabled(ShouldStartHeartbeatMonitoring.INSTANCE, new Workspace(workspaceId));
      return new DefaultReplicationWorker(
          jobRunConfig.getJobId(),
          Math.toIntExact(jobRunConfig.getAttemptId()),
          airbyteSource,
          new NamespacingMapper(syncInput.getNamespaceDefinition(), syncInput.getNamespaceFormat(), syncInput.getPrefix()),
          new DefaultAirbyteDestination(destinationLauncher,
              new VersionedAirbyteStreamFactory<>(serDeProvider, migratorFactory, destinationLauncherConfig.getProtocolVersion(),
                  Optional.of(syncInput.getCatalog()),
                  DefaultAirbyteDestination.CONTAINER_LOG_MDC_BUILDER, Optional.of(DestinationException.class)),
              new VersionedAirbyteMessageBufferedWriterFactory(serDeProvider, migratorFactory, destinationLauncherConfig.getProtocolVersion(),
                  Optional.of(syncInput.getCatalog())),
              migratorFactory.getProtocolSerializer(destinationLauncherConfig.getProtocolVersion())),
          new AirbyteMessageTracker(featureFlags),
          syncPersistenceFactory,
          new RecordSchemaValidator(WorkerUtils.mapStreamNamesToSchemas(syncInput),
              featureFlagClient.enabled(PerfBackgroundJsonValidation.INSTANCE, new Workspace(syncInput.getWorkspaceId()))),
          metricReporter,
          new ConnectorConfigUpdater(airbyteApiClient.getSourceApi(), airbyteApiClient.getDestinationApi()),
          fieldSelectionEnabled,
          heartbeatTimeoutEnabled,
          heartbeatTimeoutChaperone);
    };
  }

  @SuppressWarnings("LineLength")
  private CheckedSupplier<Worker<StandardSyncInput, ReplicationOutput>, Exception> getContainerLauncherWorkerFactory(
                                                                                                                     final ContainerOrchestratorConfig containerOrchestratorConfig,
                                                                                                                     final IntegrationLauncherConfig sourceLauncherConfig,
                                                                                                                     final IntegrationLauncherConfig destinationLauncherConfig,
                                                                                                                     final JobRunConfig jobRunConfig,
                                                                                                                     final ResourceRequirements resourceRequirements,
                                                                                                                     final Supplier<ActivityExecutionContext> activityContext,
                                                                                                                     final WorkerConfigs workerConfigs,
                                                                                                                     final UUID connectionId) {
    return () -> new ReplicationLauncherWorker(
        connectionId,
        containerOrchestratorConfig,
        sourceLauncherConfig,
        destinationLauncherConfig,
        jobRunConfig,
        resourceRequirements,
        activityContext,
        serverPort,
        temporalUtils,
        workerConfigs);
  }

  private boolean isResetJob(final String dockerImage) {
    return WorkerConstants.RESET_JOB_SOURCE_DOCKER_IMAGE_STUB.equalsIgnoreCase(dockerImage);
  }

}
