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
import io.airbyte.workers.general.ReplicationWorker;
import io.airbyte.workers.helper.ResumableFullRefreshStatsHelper;
import io.airbyte.workers.models.ReplicationActivityInput;
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
import java.util.UUID;
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
    this.metricClient = metricClient;
    this.featureFlagClient = featureFlagClient;
    this.payloadChecker = payloadChecker;
    this.stateStorageClient = stateStorageClient;
    this.catalogStorageClient = catalogStorageClient;
    this.logClientManager = logClientManager;
  }

  record TracingContext(UUID connectionId, String jobId, Long attemptNumber, Map<String, Object> traceAttributes) {}

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
  @Deprecated
  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public StandardSyncOutput replicateV2(final ReplicationActivityInput replicationActivityInput) {
    metricClient.count(OssMetricsRegistry.ACTIVITY_REPLICATION, 1);

    final TracingContext tracingContext = buildTracingContext(replicationActivityInput);
    ApmTraceUtils.addTagsToTrace(tracingContext.traceAttributes);

    if (replicationActivityInput.getIsReset()) {
      metricClient.count(OssMetricsRegistry.RESET_REQUEST, 1);
    }
    final ActivityExecutionContext context = Activity.getExecutionContext();

    final AtomicReference<Runnable> cancellationCallback = new AtomicReference<>(null);

    return HeartbeatUtils.withBackgroundHeartbeat(
        cancellationCallback,
        () -> {
          final var workerAndReplicationInput = getWorkerAndReplicationInput(replicationActivityInput);
          final ReplicationInput replicationInput = workerAndReplicationInput.replicationInput;
          final ReplicationWorker worker = workerAndReplicationInput.worker;

          LOGGER.info("connection {}, input: {}", tracingContext.connectionId, replicationInput);
          cancellationCallback.set(worker::cancel);

          final TemporalAttemptExecution temporalAttempt =
              new TemporalAttemptExecution(
                  workspaceRoot,
                  replicationInput.getJobRunConfig(),
                  worker,
                  replicationInput,
                  airbyteVersion,
                  logClientManager);

          final ReplicationOutput attemptOutput = temporalAttempt.get();
          return finalizeOutput(replicationActivityInput, attemptOutput);
        },
        context);
  }

  private StandardSyncOutput finalizeOutput(final ReplicationActivityInput replicationActivityInput, final ReplicationOutput attemptOutput) {
    final TracingContext tracingContext = buildTracingContext(replicationActivityInput);
    ApmTraceUtils.addTagsToTrace(tracingContext.traceAttributes);

    final MetricAttribute[] metricAttributes = tracingContext.traceAttributes.entrySet().stream()
        .map(e -> new MetricAttribute(ApmTraceUtils.formatTag(e.getKey()), e.getValue().toString()))
        .collect(Collectors.toSet()).toArray(new MetricAttribute[] {});

    final StandardSyncOutput standardSyncOutput = reduceReplicationOutput(attemptOutput, metricAttributes);

    final String standardSyncOutputString = standardSyncOutput.toString();
    LOGGER.info("sync summary: {}", standardSyncOutputString);
    if (standardSyncOutputString.length() > MAX_TEMPORAL_MESSAGE_SIZE) {
      LOGGER.error("Sync output exceeds the max temporal message size of {}, actual is {}.", MAX_TEMPORAL_MESSAGE_SIZE,
          standardSyncOutputString.length());
    } else {
      LOGGER.info("Sync summary length: {}", standardSyncOutputString.length());
    }

    if (featureFlagClient.boolVariation(WriteOutputCatalogToObjectStorage.INSTANCE, new Connection(tracingContext.connectionId))) {
      final var uri = catalogStorageClient.persist(
          attemptOutput.getOutputCatalog(),
          tracingContext.connectionId,
          Long.parseLong(tracingContext.jobId),
          tracingContext.attemptNumber.intValue(),
          metricAttributes);

      standardSyncOutput.setCatalogUri(uri);
    }

    payloadChecker.validatePayloadSize(standardSyncOutput, metricAttributes);

    return standardSyncOutput;
  }

  record WorkerAndReplicationInput(WorkloadApiWorker worker, ReplicationInput replicationInput) {}

  @VisibleForTesting
  WorkerAndReplicationInput getWorkerAndReplicationInput(final ReplicationActivityInput replicationActivityInput) {
    final ReplicationInput replicationInput;
    final WorkloadApiWorker worker;

    replicationInput = replicationInputHydrator.mapActivityInputToReplInput(replicationActivityInput);
    worker = new WorkloadApiWorker(jobOutputDocStore, airbyteApiClient,
        workloadApiClient, workloadClient, workloadIdGenerator, replicationActivityInput, featureFlagClient, logClientManager);

    return new WorkerAndReplicationInput(worker, replicationInput);
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

  private TracingContext buildTracingContext(final ReplicationActivityInput replicationActivityInput) {
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

    return new TracingContext(connectionId, jobId, attemptNumber, traceAttributes);
  }

}
