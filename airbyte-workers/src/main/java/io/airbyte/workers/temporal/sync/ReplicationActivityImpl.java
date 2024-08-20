/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
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
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.commons.functional.CheckedSupplier;
import io.airbyte.commons.logging.LogClientManager;
import io.airbyte.commons.temporal.HeartbeatUtils;
import io.airbyte.commons.temporal.utils.PayloadChecker;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.ReplicationAttemptSummary;
import io.airbyte.config.ReplicationOutput;
import io.airbyte.config.StandardSyncOutput;
import io.airbyte.config.StandardSyncSummary;
import io.airbyte.config.State;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.featureflag.Connection;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.WriteOutputCatalogToObjectStorage;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.persistence.job.models.ReplicationInput;
import io.airbyte.workers.ReplicationInputHydrator;
import io.airbyte.workers.Worker;
import io.airbyte.workers.helper.ResumableFullRefreshStatsHelper;
import io.airbyte.workers.models.ReplicationActivityInput;
import io.airbyte.workers.orchestrator.OrchestratorHandleFactory;
import io.airbyte.workers.storage.activities.OutputStorageClient;
import io.airbyte.workers.sync.WorkloadApiWorker;
import io.airbyte.workers.sync.WorkloadClient;
import io.airbyte.workers.temporal.TemporalAttemptExecution;
import io.airbyte.workers.workload.JobOutputDocStore;
import io.airbyte.workers.workload.WorkloadIdGenerator;
import io.airbyte.workload.api.client.WorkloadApiClient;
import io.micronaut.context.annotation.Value;
import io.temporal.activity.Activity;
import io.temporal.activity.ActivityExecutionContext;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Replication temporal activity impl.
 */
@Singleton
@SuppressWarnings("PMD.UseVarargs")
public class ReplicationActivityImpl implements ReplicationActivity {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationActivityImpl.class);
  private static final int MAX_TEMPORAL_MESSAGE_SIZE = 2 * 1024 * 1024;

  private final ReplicationInputHydrator replicationInputHydrator;
  private final Path workspaceRoot;
  private final String airbyteVersion;
  private final AirbyteApiClient airbyteApiClient;
  private final WorkloadApiClient workloadApiClient;
  private final WorkloadClient workloadClient;
  private final JobOutputDocStore jobOutputDocStore;
  private final WorkloadIdGenerator workloadIdGenerator;
  private final OrchestratorHandleFactory orchestratorHandleFactory;
  private final MetricClient metricClient;
  private final FeatureFlagClient featureFlagClient;
  private final PayloadChecker payloadChecker;
  private final OutputStorageClient<State> stateStorageClient;
  private final OutputStorageClient<ConfiguredAirbyteCatalog> catalogStorageClient;
  private final LogClientManager logClientManager;

  public ReplicationActivityImpl(final SecretsRepositoryReader secretsRepositoryReader,
                                 @Named("workspaceRoot") final Path workspaceRoot,
                                 @Value("${airbyte.version}") final String airbyteVersion,
                                 final AirbyteApiClient airbyteApiClient,
                                 final JobOutputDocStore jobOutputDocStore,
                                 final WorkloadApiClient workloadApiClient,
                                 final WorkloadClient workloadClient,
                                 final WorkloadIdGenerator workloadIdGenerator,
                                 final OrchestratorHandleFactory orchestratorHandleFactory,
                                 final MetricClient metricClient,
                                 final FeatureFlagClient featureFlagClient,
                                 final PayloadChecker payloadChecker,
                                 @Named("outputStateClient") final OutputStorageClient<State> stateStorageClient,
                                 @Named("outputCatalogClient") final OutputStorageClient<ConfiguredAirbyteCatalog> catalogStorageClient,
                                 final ResumableFullRefreshStatsHelper resumableFullRefreshStatsHelper,
                                 final LogClientManager logClientManager) {
    this.replicationInputHydrator = new ReplicationInputHydrator(airbyteApiClient, resumableFullRefreshStatsHelper, secretsRepositoryReader,
        featureFlagClient);
    this.workspaceRoot = workspaceRoot;
    this.airbyteVersion = airbyteVersion;
    this.airbyteApiClient = airbyteApiClient;
    this.jobOutputDocStore = jobOutputDocStore;
    this.workloadApiClient = workloadApiClient;
    this.workloadClient = workloadClient;
    this.workloadIdGenerator = workloadIdGenerator;
    this.orchestratorHandleFactory = orchestratorHandleFactory;
    this.metricClient = metricClient;
    this.featureFlagClient = featureFlagClient;
    this.payloadChecker = payloadChecker;
    this.stateStorageClient = stateStorageClient;
    this.catalogStorageClient = catalogStorageClient;
    this.logClientManager = logClientManager;
  }

  @VisibleForTesting
  ReplicationActivityImpl(final ReplicationInputHydrator replicationInputHydrator,
                          @Named("workspaceRoot") final Path workspaceRoot,
                          @Value("${airbyte.version}") final String airbyteVersion,
                          final AirbyteApiClient airbyteApiClient,
                          final JobOutputDocStore jobOutputDocStore,
                          final WorkloadApiClient workloadApiClient,
                          final WorkloadClient workloadClient,
                          final WorkloadIdGenerator workloadIdGenerator,
                          final OrchestratorHandleFactory orchestratorHandleFactory,
                          final MetricClient metricClient,
                          final FeatureFlagClient featureFlagClient,
                          final PayloadChecker payloadChecker,
                          @Named("outputStateClient") final OutputStorageClient<State> stateStorageClient,
                          @Named("outputCatalogClient") final OutputStorageClient<ConfiguredAirbyteCatalog> catalogStorageClient,
                          final LogClientManager logClientManager) {
    this.replicationInputHydrator = replicationInputHydrator;
    this.workspaceRoot = workspaceRoot;
    this.airbyteVersion = airbyteVersion;
    this.airbyteApiClient = airbyteApiClient;
    this.jobOutputDocStore = jobOutputDocStore;
    this.workloadApiClient = workloadApiClient;
    this.workloadClient = workloadClient;
    this.workloadIdGenerator = workloadIdGenerator;
    this.orchestratorHandleFactory = orchestratorHandleFactory;
    this.metricClient = metricClient;
    this.featureFlagClient = featureFlagClient;
    this.payloadChecker = payloadChecker;
    this.stateStorageClient = stateStorageClient;
    this.catalogStorageClient = catalogStorageClient;
    this.logClientManager = logClientManager;
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
    metricClient.count(OssMetricsRegistry.ACTIVITY_REPLICATION, 1);

    final var connectionId = replicationActivityInput.getConnectionId();
    final var jobId = replicationActivityInput.getJobRunConfig().getJobId();
    final var attemptNumber = replicationActivityInput.getJobRunConfig().getAttemptId();

    final Map<String, Object> traceAttributes =
        Map.of(
            CONNECTION_ID_KEY, connectionId,
            JOB_ID_KEY, jobId,
            ATTEMPT_NUMBER_KEY, attemptNumber,
            DESTINATION_DOCKER_IMAGE_KEY, replicationActivityInput.getDestinationLauncherConfig().getDockerImage(),
            SOURCE_DOCKER_IMAGE_KEY, replicationActivityInput.getSourceLauncherConfig().getDockerImage());
    ApmTraceUtils
        .addTagsToTrace(traceAttributes);

    final MetricAttribute[] metricAttributes = traceAttributes.entrySet().stream()
        .map(e -> new MetricAttribute(ApmTraceUtils.formatTag(e.getKey()), e.getValue().toString()))
        .collect(Collectors.toSet()).toArray(new MetricAttribute[] {});

    if (replicationActivityInput.getIsReset()) {
      metricClient.count(OssMetricsRegistry.RESET_REQUEST, 1);
    }
    final ActivityExecutionContext context = Activity.getExecutionContext();

    final AtomicReference<Runnable> cancellationCallback = new AtomicReference<>(null);

    return HeartbeatUtils.withBackgroundHeartbeat(
        cancellationCallback,
        () -> {
          final var workerAndReplicationInput = getWorkerAndReplicationInput(replicationActivityInput, context);
          final ReplicationInput hydratedReplicationInput = workerAndReplicationInput.replicationInput;
          final Worker<ReplicationInput, ReplicationOutput> worker = workerAndReplicationInput.worker;

          LOGGER.info("connection {}, hydrated input: {}", replicationActivityInput.getConnectionId(), hydratedReplicationInput);
          cancellationCallback.set(worker::cancel);

          final TemporalAttemptExecution<ReplicationInput, ReplicationOutput> temporalAttempt =
              new TemporalAttemptExecution<>(
                  workspaceRoot,
                  hydratedReplicationInput.getJobRunConfig(),
                  worker,
                  hydratedReplicationInput,
                  airbyteApiClient,
                  airbyteVersion,
                  () -> context,
                  Optional.ofNullable(replicationActivityInput.getTaskQueue()),
                  logClientManager);

          final ReplicationOutput attemptOutput = temporalAttempt.get();
          final StandardSyncOutput standardSyncOutput = reduceReplicationOutput(attemptOutput, metricAttributes);

          final String standardSyncOutputString = standardSyncOutput.toString();
          LOGGER.info("sync summary: {}", standardSyncOutputString);
          if (standardSyncOutputString.length() > MAX_TEMPORAL_MESSAGE_SIZE) {
            LOGGER.error("Sync output exceeds the max temporal message size of {}, actual is {}.", MAX_TEMPORAL_MESSAGE_SIZE,
                standardSyncOutputString.length());
          } else {
            LOGGER.info("Sync summary length: {}", standardSyncOutputString.length());
          }

          if (featureFlagClient.boolVariation(WriteOutputCatalogToObjectStorage.INSTANCE, new Connection(connectionId))) {
            final var uri = catalogStorageClient.persist(
                attemptOutput.getOutputCatalog(),
                connectionId,
                Long.parseLong(jobId),
                attemptNumber.intValue(),
                metricAttributes);

            standardSyncOutput.setCatalogUri(uri);
          }

          payloadChecker.validatePayloadSize(standardSyncOutput, metricAttributes);

          return standardSyncOutput;
        },
        context);
  }

  record WorkerAndReplicationInput(Worker<ReplicationInput, ReplicationOutput> worker, ReplicationInput replicationInput) {}

  @VisibleForTesting
  WorkerAndReplicationInput getWorkerAndReplicationInput(final ReplicationActivityInput replicationActivityInput,
                                                         final ActivityExecutionContext context)
      throws Exception {
    final ReplicationInput hydratedReplicationInput;
    final Worker<ReplicationInput, ReplicationOutput> worker;

    if (useWorkloadApi(replicationActivityInput)) {
      hydratedReplicationInput = replicationInputHydrator.mapActivityInputToReplInput(replicationActivityInput);
      worker = new WorkloadApiWorker(jobOutputDocStore, airbyteApiClient,
          workloadApiClient, workloadClient, workloadIdGenerator, replicationActivityInput, featureFlagClient, logClientManager);
    } else {
      hydratedReplicationInput = replicationInputHydrator.getHydratedReplicationInput(replicationActivityInput);
      final CheckedSupplier<Worker<ReplicationInput, ReplicationOutput>, Exception> workerFactory =
          orchestratorHandleFactory.create(hydratedReplicationInput.getSourceLauncherConfig(),
              hydratedReplicationInput.getDestinationLauncherConfig(), hydratedReplicationInput.getJobRunConfig(), hydratedReplicationInput,
              () -> context);
      worker = workerFactory.get();
    }

    return new WorkerAndReplicationInput(worker, hydratedReplicationInput);
  }

  private StandardSyncOutput reduceReplicationOutput(final ReplicationOutput output, final MetricAttribute[] metricAttributes) {
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
    syncSummary.setStreamCount((long) output.getOutputCatalog().getStreams().size());

    standardSyncOutput.setStandardSyncSummary(syncSummary);
    standardSyncOutput.setFailures(output.getFailures());

    return standardSyncOutput;
  }

  @VisibleForTesting
  boolean useWorkloadApi(final ReplicationActivityInput input) {
    // TODO: remove this once active workloads finish
    if (input.getUseWorkloadApi() == null) {
      return false;
    } else {
      return input.getUseWorkloadApi();
    }
  }

  private void traceReplicationSummary(final ReplicationAttemptSummary replicationSummary, final MetricAttribute[] metricAttributes) {
    if (replicationSummary == null) {
      return;
    }

    final Map<String, Object> tags = new HashMap<>();
    if (replicationSummary.getBytesSynced() != null) {
      tags.put(REPLICATION_BYTES_SYNCED_KEY, replicationSummary.getBytesSynced());
      metricClient.count(OssMetricsRegistry.REPLICATION_BYTES_SYNCED, replicationSummary.getBytesSynced(), metricAttributes);
    }
    if (replicationSummary.getRecordsSynced() != null) {
      tags.put(REPLICATION_RECORDS_SYNCED_KEY, replicationSummary.getRecordsSynced());
      metricClient.count(OssMetricsRegistry.REPLICATION_RECORDS_SYNCED, replicationSummary.getRecordsSynced(), metricAttributes);
    }
    if (replicationSummary.getStatus() != null) {
      tags.put(REPLICATION_STATUS_KEY, replicationSummary.getStatus().value());
    }
    if (!tags.isEmpty()) {
      ApmTraceUtils.addTagsToTrace(tags);
    }
  }

}
