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

import com.google.common.annotations.VisibleForTesting;
import datadog.trace.api.Trace;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.ConnectionRead;
import io.airbyte.api.client.model.generated.ConnectionState;
import io.airbyte.api.client.model.generated.ConnectionStateType;
import io.airbyte.api.client.model.generated.JobOptionalRead;
import io.airbyte.api.client.model.generated.StreamDescriptor;
import io.airbyte.commons.converters.CatalogClientConverters;
import io.airbyte.commons.converters.ProtocolConverters;
import io.airbyte.commons.converters.StateConverter;
import io.airbyte.commons.functional.CheckedSupplier;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.protocol.CatalogTransforms;
import io.airbyte.commons.temporal.TemporalUtils;
import io.airbyte.commons.temporal.utils.PayloadChecker;
import io.airbyte.config.AirbyteConfigValidator;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.Configs.WorkerEnvironment;
import io.airbyte.config.ReplicationAttemptSummary;
import io.airbyte.config.ReplicationOutput;
import io.airbyte.config.StandardSyncInput;
import io.airbyte.config.StandardSyncOutput;
import io.airbyte.config.StandardSyncSummary;
import io.airbyte.config.State;
import io.airbyte.config.helpers.LogConfigs;
import io.airbyte.config.helpers.StateMessageHelper;
import io.airbyte.config.persistence.split_secrets.SecretsHydrator;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.RemoveLargeSyncInputs;
import io.airbyte.featureflag.Workspace;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.persistence.job.models.ReplicationInput;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.workers.Worker;
import io.airbyte.workers.helpers.BackfillHelper;
import io.airbyte.workers.models.RefreshSchemaActivityOutput;
import io.airbyte.workers.models.ReplicationActivityInput;
import io.airbyte.workers.orchestrator.OrchestratorHandleFactory;
import io.airbyte.workers.temporal.TemporalAttemptExecution;
import io.micronaut.context.annotation.Value;
import io.temporal.activity.Activity;
import io.temporal.activity.ActivityExecutionContext;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Replication temporal activity impl.
 */
@Singleton
public class ReplicationActivityImpl implements ReplicationActivity {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationActivityImpl.class);
  private static final int MAX_TEMPORAL_MESSAGE_SIZE = 2 * 1024 * 1024;

  private final SecretsHydrator secretsHydrator;
  private final Path workspaceRoot;
  private final WorkerEnvironment workerEnvironment;
  private final LogConfigs logConfigs;
  private final String airbyteVersion;
  private final AirbyteConfigValidator airbyteConfigValidator;
  private final TemporalUtils temporalUtils;
  private final AirbyteApiClient airbyteApiClient;
  private final OrchestratorHandleFactory orchestratorHandleFactory;
  private final MetricClient metricClient;
  private final FeatureFlagClient featureFlagClient;

  public ReplicationActivityImpl(final SecretsHydrator secretsHydrator,
                                 @Named("workspaceRoot") final Path workspaceRoot,
                                 final WorkerEnvironment workerEnvironment,
                                 final LogConfigs logConfigs,
                                 @Value("${airbyte.version}") final String airbyteVersion,
                                 final AirbyteConfigValidator airbyteConfigValidator,
                                 final TemporalUtils temporalUtils,
                                 final AirbyteApiClient airbyteApiClient,
                                 final OrchestratorHandleFactory orchestratorHandleFactory,
                                 final MetricClient metricClient,
                                 final FeatureFlagClient featureFlagClient) {
    this.secretsHydrator = secretsHydrator;
    this.workspaceRoot = workspaceRoot;
    this.workerEnvironment = workerEnvironment;
    this.logConfigs = logConfigs;
    this.airbyteVersion = airbyteVersion;
    this.airbyteConfigValidator = airbyteConfigValidator;
    this.temporalUtils = temporalUtils;
    this.airbyteApiClient = airbyteApiClient;
    this.orchestratorHandleFactory = orchestratorHandleFactory;
    this.metricClient = metricClient;
    this.featureFlagClient = featureFlagClient;
  }

  /**
   * Performs the replication activity.
   * <p>
   * Takes a lite input (no catalog, no state, no secrets) to avoid passing those through Temporal and
   * hydrates it before launching the replication orchestrator.
   * <p>
   * TODO: this is the preferred method. Once we remove `replicate`, this can be renamed.
   *
   * @param replicationActivityInput the input to the replication activity
   * @return output from the replication activity, populated in the StandardSyncOutput
   */
  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public StandardSyncOutput replicateV2(final ReplicationActivityInput replicationActivityInput) {
    if (!featureFlagClient.boolVariation(RemoveLargeSyncInputs.INSTANCE, new Workspace(replicationActivityInput.getWorkspaceId()))) {
      return replicate(
          replicationActivityInput.getJobRunConfig(),
          replicationActivityInput.getSourceLauncherConfig(),
          replicationActivityInput.getDestinationLauncherConfig(),
          replicationActivityInput.getSyncInput(),
          replicationActivityInput.getTaskQueue());
    }
    metricClient.count(OssMetricsRegistry.ACTIVITY_REPLICATION, 1);
    final Map<String, Object> traceAttributes =
        Map.of(
            ATTEMPT_NUMBER_KEY, replicationActivityInput.getJobRunConfig().getAttemptId(),
            CONNECTION_ID_KEY, replicationActivityInput.getConnectionId(),
            JOB_ID_KEY, replicationActivityInput.getJobRunConfig().getJobId(),
            DESTINATION_DOCKER_IMAGE_KEY, replicationActivityInput.getDestinationLauncherConfig().getDockerImage(),
            SOURCE_DOCKER_IMAGE_KEY, replicationActivityInput.getSourceLauncherConfig().getDockerImage());
    ApmTraceUtils
        .addTagsToTrace(traceAttributes);
    if (replicationActivityInput.getIsReset()) {
      metricClient.count(OssMetricsRegistry.RESET_REQUEST, 1);
    }
    final ActivityExecutionContext context = Activity.getExecutionContext();

    final AtomicReference<Runnable> cancellationCallback = new AtomicReference<>(null);

    return temporalUtils.withBackgroundHeartbeat(
        cancellationCallback,
        () -> {
          final ReplicationInput hydratedReplicationInput = getHydratedReplicationInput(replicationActivityInput);
          LOGGER.info("replicationInput: {}", hydratedReplicationInput);
          final CheckedSupplier<Worker<ReplicationInput, ReplicationOutput>, Exception> workerFactory =
              orchestratorHandleFactory.create(hydratedReplicationInput.getSourceLauncherConfig(),
                  hydratedReplicationInput.getDestinationLauncherConfig(), hydratedReplicationInput.getJobRunConfig(), hydratedReplicationInput,
                  () -> context);
          final var worker = workerFactory.get();
          cancellationCallback.set(worker::cancel);

          final TemporalAttemptExecution<ReplicationInput, ReplicationOutput> temporalAttempt =
              new TemporalAttemptExecution<>(
                  workspaceRoot,
                  workerEnvironment,
                  logConfigs,
                  hydratedReplicationInput.getJobRunConfig(),
                  worker,
                  hydratedReplicationInput,
                  airbyteApiClient,
                  airbyteVersion,
                  () -> context,
                  Optional.ofNullable(replicationActivityInput.getTaskQueue()));

          final ReplicationOutput attemptOutput = temporalAttempt.get();
          final StandardSyncOutput standardSyncOutput = reduceReplicationOutput(attemptOutput, traceAttributes);

          final String standardSyncOutputString = standardSyncOutput.toString();
          LOGGER.info("sync summary: {}", standardSyncOutputString);
          if (standardSyncOutputString.length() > MAX_TEMPORAL_MESSAGE_SIZE) {
            LOGGER.error("Sync output exceeds the max temporal message size of {}, actual is {}.", MAX_TEMPORAL_MESSAGE_SIZE,
                standardSyncOutputString.length());
          } else {
            LOGGER.info("Sync summary length: {}", standardSyncOutputString.length());
          }
          List<StreamDescriptor> streamsToBackfill = List.of();
          if (replicationActivityInput.getSchemaRefreshOutput() != null) {
            streamsToBackfill = BackfillHelper
                .getStreamsToBackfill(replicationActivityInput.getSchemaRefreshOutput().getAppliedDiff(), hydratedReplicationInput.getCatalog());
          }
          BackfillHelper.markBackfilledStreams(streamsToBackfill, standardSyncOutput);
          return standardSyncOutput;
        },
        context);
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
    metricClient.count(OssMetricsRegistry.ACTIVITY_REPLICATION, 1);

    final Map<String, Object> traceAttributes =
        Map.of(
            ATTEMPT_NUMBER_KEY, jobRunConfig.getAttemptId(),
            CONNECTION_ID_KEY, syncInput.getConnectionId(),
            JOB_ID_KEY, jobRunConfig.getJobId(),
            DESTINATION_DOCKER_IMAGE_KEY, destinationLauncherConfig.getDockerImage(),
            SOURCE_DOCKER_IMAGE_KEY, sourceLauncherConfig.getDockerImage());
    ApmTraceUtils
        .addTagsToTrace(traceAttributes);
    if (syncInput.getIsReset()) {
      metricClient.count(OssMetricsRegistry.RESET_REQUEST, 1);
    }
    final ActivityExecutionContext context = Activity.getExecutionContext();

    final AtomicReference<Runnable> cancellationCallback = new AtomicReference<>(null);

    return PayloadChecker.validatePayloadSize(temporalUtils.withBackgroundHeartbeat(
        cancellationCallback,
        () -> {
          final var hydratedSyncInput = getHydratedSyncInput(syncInput);
          final var replicationInput =
              getReplicationInputFromSyncInput(hydratedSyncInput, jobRunConfig, sourceLauncherConfig, destinationLauncherConfig);
          final CheckedSupplier<Worker<ReplicationInput, ReplicationOutput>, Exception> workerFactory =
              orchestratorHandleFactory.create(sourceLauncherConfig, destinationLauncherConfig, jobRunConfig, replicationInput, () -> context);
          final var worker = workerFactory.get();
          cancellationCallback.set(worker::cancel);

          final TemporalAttemptExecution<ReplicationInput, ReplicationOutput> temporalAttempt =
              new TemporalAttemptExecution<>(
                  workspaceRoot,
                  workerEnvironment,
                  logConfigs,
                  jobRunConfig,
                  worker,
                  replicationInput,
                  airbyteApiClient,
                  airbyteVersion,
                  () -> context,
                  Optional.ofNullable(taskQueue));

          final ReplicationOutput attemptOutput = temporalAttempt.get();
          final StandardSyncOutput standardSyncOutput = reduceReplicationOutput(attemptOutput, traceAttributes);

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
        context));
  }

  private StandardSyncInput getHydratedSyncInput(final StandardSyncInput syncInput) {
    final var fullSourceConfig = secretsHydrator.hydrate(syncInput.getSourceConfiguration());
    final var fullDestinationConfig = secretsHydrator.hydrate(syncInput.getDestinationConfiguration());

    final var fullSyncInput = Jsons.clone(syncInput)
        .withSourceConfiguration(fullSourceConfig)
        .withDestinationConfiguration(fullDestinationConfig);

    airbyteConfigValidator.ensureAsRuntime(ConfigSchema.STANDARD_SYNC_INPUT, Jsons.jsonNode(fullSyncInput));
    return fullSyncInput;
  }

  /**
   * Converts a ReplicationActivityInput -- passed through Temporal to the replication activity -- to
   * a ReplicationInput which will be passed down the stack to the actual
   * source/destination/orchestrator processes.
   *
   * @param replicationActivityInput the input passed from the sync workflow to the replication
   *        activity
   * @return the input to be passed down to the source/destination/orchestrator processes
   * @throws Exception from the Airbyte API
   */
  @VisibleForTesting
  protected ReplicationInput getHydratedReplicationInput(final ReplicationActivityInput replicationActivityInput) throws Exception {
    final ConfiguredAirbyteCatalog catalog = retrieveCatalog(replicationActivityInput);
    if (replicationActivityInput.getIsReset()) {
      // If this is a reset, we need to set the streams being reset to Full Refresh | Overwrite.
      updateCatalogForReset(replicationActivityInput, catalog);
    }
    // Retrieve the state.
    State state = retrieveState(replicationActivityInput);
    if (replicationActivityInput.getSchemaRefreshOutput() != null) {
      state = getUpdatedStateForBackfill(state, replicationActivityInput.getSchemaRefreshOutput(), catalog);
    }
    // Hydrate the secrets.
    final var fullSourceConfig = secretsHydrator.hydrate(replicationActivityInput.getSourceConfiguration());
    final var fullDestinationConfig = secretsHydrator.hydrate(replicationActivityInput.getDestinationConfiguration());
    return new ReplicationInput()
        .withNamespaceDefinition(replicationActivityInput.getNamespaceDefinition())
        .withNamespaceFormat(replicationActivityInput.getNamespaceFormat())
        .withPrefix(replicationActivityInput.getPrefix())
        .withSourceId(replicationActivityInput.getSourceId())
        .withDestinationId(replicationActivityInput.getDestinationId())
        .withSourceConfiguration(fullSourceConfig)
        .withDestinationConfiguration(fullDestinationConfig)
        .withSyncResourceRequirements(replicationActivityInput.getSyncResourceRequirements())
        .withWorkspaceId(replicationActivityInput.getWorkspaceId())
        .withConnectionId(replicationActivityInput.getConnectionId())
        .withNormalizeInDestinationContainer(replicationActivityInput.getNormalizeInDestinationContainer())
        .withIsReset(replicationActivityInput.getIsReset())
        .withJobRunConfig(replicationActivityInput.getJobRunConfig())
        .withSourceLauncherConfig(replicationActivityInput.getSourceLauncherConfig())
        .withDestinationLauncherConfig(replicationActivityInput.getDestinationLauncherConfig())
        .withCatalog(catalog)
        .withState(state);
  }

  private State getUpdatedStateForBackfill(State state, RefreshSchemaActivityOutput schemaRefreshOutput, final ConfiguredAirbyteCatalog catalog) {
    if (schemaRefreshOutput != null && schemaRefreshOutput.getAppliedDiff() != null) {
      final var streamsToBackfill = BackfillHelper.getStreamsToBackfill(schemaRefreshOutput.getAppliedDiff(), catalog);
      LOGGER.debug("Backfilling streams: {}", String.join(", ", streamsToBackfill.stream().map(StreamDescriptor::getName).toList()));
      return BackfillHelper.clearStateForStreamsToBackfill(state, streamsToBackfill);
    }
    // No schema refresh output, so we just return the original state.
    return state;
  }

  @NotNull
  private ConfiguredAirbyteCatalog retrieveCatalog(ReplicationActivityInput replicationActivityInput) throws Exception {
    final ConnectionRead connectionInfo =
        AirbyteApiClient
            .retryWithJitterThrows(
                () -> airbyteApiClient.getConnectionApi()
                    .getConnection(new ConnectionIdRequestBody().connectionId(replicationActivityInput.getConnectionId())),
                "retrieve the connection");
    if (connectionInfo.getSyncCatalog() == null) {
      throw new IllegalArgumentException("Connection is missing catalog, which is required");
    }
    final ConfiguredAirbyteCatalog catalog = CatalogClientConverters.toConfiguredAirbyteProtocol(connectionInfo.getSyncCatalog());
    return catalog;
  }

  private State retrieveState(ReplicationActivityInput replicationActivityInput) throws Exception {
    final ConnectionState connectionState = AirbyteApiClient.retryWithJitterThrows(
        () -> airbyteApiClient.getStateApi().getState(new ConnectionIdRequestBody().connectionId(replicationActivityInput.getConnectionId())),
        "retrieve the state");
    final State state =
        connectionState != null && !ConnectionStateType.NOT_SET.equals(connectionState.getStateType())
            ? StateMessageHelper.getState(StateConverter.toInternal(StateConverter.fromClientToApi(connectionState)))
            : null;
    return state;
  }

  private void updateCatalogForReset(ReplicationActivityInput replicationActivityInput, ConfiguredAirbyteCatalog catalog) throws Exception {
    final JobOptionalRead jobInfo = AirbyteApiClient.retryWithJitterThrows(
        () -> airbyteApiClient.getJobsApi().getLastReplicationJob(
            new ConnectionIdRequestBody().connectionId(replicationActivityInput.getConnectionId())),
        "get job info to retrieve streams to reset");
    final boolean hasStreamsToReset = jobInfo != null && jobInfo.getJob() != null && jobInfo.getJob().getResetConfig() != null
        && jobInfo.getJob().getResetConfig().getStreamsToReset() != null;
    if (hasStreamsToReset) {
      final var streamsToReset =
          jobInfo.getJob().getResetConfig().getStreamsToReset().stream().map(ProtocolConverters::clientStreamDescriptorToProtocol).toList();
      CatalogTransforms.updateCatalogForReset(streamsToReset, catalog);
    }
  }

  // Simple converter from StandardSyncInput to ReplicationInput.
  // TODO: remove when the workflow version that passes a StandardSyncInput is removed.
  private ReplicationInput getReplicationInputFromSyncInput(StandardSyncInput hydratedSyncInput,
                                                            final JobRunConfig jobRunConfig,
                                                            final IntegrationLauncherConfig sourceLauncherConfig,
                                                            final IntegrationLauncherConfig destinationLauncherConfig) {
    return new ReplicationInput()
        .withNamespaceDefinition(hydratedSyncInput.getNamespaceDefinition())
        .withNamespaceFormat(hydratedSyncInput.getNamespaceFormat())
        .withPrefix(hydratedSyncInput.getPrefix())
        .withSourceId(hydratedSyncInput.getSourceId())
        .withDestinationId(hydratedSyncInput.getDestinationId())
        .withSourceConfiguration(hydratedSyncInput.getSourceConfiguration())
        .withDestinationConfiguration(hydratedSyncInput.getDestinationConfiguration())
        .withSyncResourceRequirements(hydratedSyncInput.getSyncResourceRequirements())
        .withWorkspaceId(hydratedSyncInput.getWorkspaceId())
        .withConnectionId(hydratedSyncInput.getConnectionId())
        .withNormalizeInDestinationContainer(hydratedSyncInput.getNormalizeInDestinationContainer())
        .withIsReset(hydratedSyncInput.getIsReset())
        .withJobRunConfig(jobRunConfig)
        .withSourceLauncherConfig(sourceLauncherConfig)
        .withDestinationLauncherConfig(destinationLauncherConfig)
        .withCatalog(hydratedSyncInput.getCatalog())
        .withState(hydratedSyncInput.getState());
  }

  private StandardSyncOutput reduceReplicationOutput(final ReplicationOutput output, final Map<String, Object> metricAttributes) {
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
    syncSummary.setPerformanceMetrics(output.getReplicationAttemptSummary().getPerformanceMetrics());

    standardSyncOutput.setState(output.getState());
    standardSyncOutput.setOutputCatalog(output.getOutputCatalog());
    standardSyncOutput.setStandardSyncSummary(syncSummary);
    standardSyncOutput.setFailures(output.getFailures());

    return standardSyncOutput;
  }

  private void traceReplicationSummary(final ReplicationAttemptSummary replicationSummary, final Map<String, Object> metricAttributes) {
    if (replicationSummary == null) {
      return;
    }

    final MetricAttribute[] attributes = metricAttributes.entrySet().stream()
        .map(e -> new MetricAttribute(ApmTraceUtils.formatTag(e.getKey()), e.getValue().toString()))
        .collect(Collectors.toSet()).toArray(new MetricAttribute[] {});
    final Map<String, Object> tags = new HashMap<>();
    if (replicationSummary.getBytesSynced() != null) {
      tags.put(REPLICATION_BYTES_SYNCED_KEY, replicationSummary.getBytesSynced());
      metricClient.count(OssMetricsRegistry.REPLICATION_BYTES_SYNCED, replicationSummary.getBytesSynced(), attributes);
    }
    if (replicationSummary.getRecordsSynced() != null) {
      tags.put(REPLICATION_RECORDS_SYNCED_KEY, replicationSummary.getRecordsSynced());
      metricClient.count(OssMetricsRegistry.REPLICATION_RECORDS_SYNCED, replicationSummary.getRecordsSynced(), attributes);
    }
    if (replicationSummary.getStatus() != null) {
      tags.put(REPLICATION_STATUS_KEY, replicationSummary.getStatus().value());
    }
    if (!tags.isEmpty()) {
      ApmTraceUtils.addTagsToTrace(tags);
    }
  }

}
