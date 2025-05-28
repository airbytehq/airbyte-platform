/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
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
import io.airbyte.commons.logging.LogClientManager;
import io.airbyte.commons.logging.LogSource;
import io.airbyte.commons.logging.MdcScope;
import io.airbyte.commons.temporal.TemporalUtils;
import io.airbyte.commons.temporal.utils.PayloadChecker;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.ReplicationAttemptSummary;
import io.airbyte.config.ReplicationOutput;
import io.airbyte.config.StandardSyncOutput;
import io.airbyte.config.StandardSyncSummary;
import io.airbyte.featureflag.Connection;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.WriteOutputCatalogToObjectStorage;
import io.airbyte.metrics.MetricAttribute;
import io.airbyte.metrics.MetricClient;
import io.airbyte.metrics.OssMetricsRegistry;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.persistence.job.models.ReplicationInput;
import io.airbyte.workers.input.ReplicationInputMapper;
import io.airbyte.workers.models.ReplicationActivityInput;
import io.airbyte.workers.storage.activities.OutputStorageClient;
import io.airbyte.workers.sync.WorkloadApiWorker;
import io.airbyte.workers.sync.WorkloadClient;
import io.airbyte.workers.workload.DataplaneGroupResolver;
import io.airbyte.workers.workload.WorkloadIdGenerator;
import io.airbyte.workers.workload.WorkloadOutputWriter;
import io.airbyte.workload.api.client.WorkloadApiClient;
import io.temporal.activity.Activity;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Replication temporal activity impl.
 */
@Singleton
@SuppressWarnings("PMD.UseVarargs")
public class AsyncReplicationActivityImpl implements AsyncReplicationActivity {

  private static final Logger LOGGER = LoggerFactory.getLogger(AsyncReplicationActivityImpl.class);
  private static final int MAX_TEMPORAL_MESSAGE_SIZE = 2 * 1024 * 1024;

  private final ReplicationInputMapper replicationInputMapper;
  private final Path workspaceRoot;
  private final WorkloadApiClient workloadApiClient;
  private final WorkloadClient workloadClient;
  private final WorkloadOutputWriter workloadOutputWriter;
  private final WorkloadIdGenerator workloadIdGenerator;
  private final MetricClient metricClient;
  private final FeatureFlagClient featureFlagClient;
  private final PayloadChecker payloadChecker;
  private final OutputStorageClient<ConfiguredAirbyteCatalog> catalogStorageClient;
  private final LogClientManager logClientManager;
  private final DataplaneGroupResolver dataplaneGroupResolver;

  public AsyncReplicationActivityImpl(
                                      @Named("workspaceRoot") final Path workspaceRoot,
                                      final WorkloadOutputWriter workloadOutputWriter,
                                      final WorkloadApiClient workloadApiClient,
                                      final WorkloadClient workloadClient,
                                      final WorkloadIdGenerator workloadIdGenerator,
                                      final MetricClient metricClient,
                                      final FeatureFlagClient featureFlagClient,
                                      final PayloadChecker payloadChecker,
                                      @Named("outputCatalogClient") final OutputStorageClient<ConfiguredAirbyteCatalog> catalogStorageClient,
                                      final LogClientManager logClientManager,
                                      final DataplaneGroupResolver dataplaneGroupResolver) {
    this.replicationInputMapper = new ReplicationInputMapper();
    this.workspaceRoot = workspaceRoot;
    this.workloadOutputWriter = workloadOutputWriter;
    this.workloadApiClient = workloadApiClient;
    this.workloadClient = workloadClient;
    this.workloadIdGenerator = workloadIdGenerator;
    this.metricClient = metricClient;
    this.featureFlagClient = featureFlagClient;
    this.payloadChecker = payloadChecker;
    this.catalogStorageClient = catalogStorageClient;
    this.logClientManager = logClientManager;
    this.dataplaneGroupResolver = dataplaneGroupResolver;
  }

  @VisibleForTesting
  AsyncReplicationActivityImpl(final ReplicationInputMapper replicationInputMapper,
                               @Named("workspaceRoot") final Path workspaceRoot,
                               final WorkloadOutputWriter workloadOutputWriter,
                               final WorkloadApiClient workloadApiClient,
                               final WorkloadClient workloadClient,
                               final WorkloadIdGenerator workloadIdGenerator,
                               final MetricClient metricClient,
                               final FeatureFlagClient featureFlagClient,
                               final PayloadChecker payloadChecker,
                               @Named("outputCatalogClient") final OutputStorageClient<ConfiguredAirbyteCatalog> catalogStorageClient,
                               final LogClientManager logClientManager,
                               final DataplaneGroupResolver dataplaneGroupResolver) {
    this.replicationInputMapper = replicationInputMapper;
    this.workspaceRoot = workspaceRoot;
    this.workloadOutputWriter = workloadOutputWriter;
    this.workloadApiClient = workloadApiClient;
    this.workloadClient = workloadClient;
    this.workloadIdGenerator = workloadIdGenerator;
    this.metricClient = metricClient;
    this.featureFlagClient = featureFlagClient;
    this.payloadChecker = payloadChecker;
    this.catalogStorageClient = catalogStorageClient;
    this.logClientManager = logClientManager;
    this.dataplaneGroupResolver = dataplaneGroupResolver;
  }

  record TracingContext(UUID connectionId, String jobId, Long attemptNumber, Map<String, Object> traceAttributes) {}

  @SuppressWarnings({"PMD.UnusedLocalVariable"})
  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public String startReplication(final ReplicationActivityInput replicationActivityInput) {
    final TracingContext tracingContext = buildTracingContext(replicationActivityInput);
    final Path jobRoot = TemporalUtils.getJobRoot(workspaceRoot, tracingContext.jobId, tracingContext.attemptNumber);

    try (final var mdcScope = new MdcScope.Builder()
        .setExtraMdcEntries(LogSource.PLATFORM.toMdc())
        .build()) {
      logClientManager.setJobMdc(jobRoot);
      metricClient.count(OssMetricsRegistry.ACTIVITY_REPLICATION);

      ApmTraceUtils.addTagsToTrace(tracingContext.traceAttributes);

      if (replicationActivityInput.isReset()) {
        metricClient.count(OssMetricsRegistry.RESET_REQUEST);
      }

      LOGGER.info("Starting async replication");

      final var workerAndReplicationInput = getWorkerAndReplicationInput(replicationActivityInput);
      final WorkloadApiWorker worker = workerAndReplicationInput.worker;

      LOGGER.debug("connection {}, input: {}", tracingContext.connectionId, workerAndReplicationInput.replicationInput);

      return worker.createWorkload(workerAndReplicationInput.replicationInput, jobRoot);
    } catch (final Exception e) {
      ApmTraceUtils.addActualRootCauseToTrace(e);
      throw Activity.wrap(e);
    } finally {
      logClientManager.setJobMdc(null);
    }
  }

  @Override
  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  public void cancel(final ReplicationActivityInput replicationActivityInput, final String workloadId) {
    final TracingContext tracingContext = buildTracingContext(replicationActivityInput);
    final Path jobRoot = TemporalUtils.getJobRoot(workspaceRoot, tracingContext.jobId, tracingContext.attemptNumber);

    try (final var ignored = new MdcScope.Builder().setExtraMdcEntries(LogSource.PLATFORM.toMdc()).build()) {
      logClientManager.setJobMdc(jobRoot);

      LOGGER.info("Canceling workload {}", workloadId);

      final var workerAndReplicationInput = getWorkerAndReplicationInput(replicationActivityInput);
      final WorkloadApiWorker worker = workerAndReplicationInput.worker;
      try {
        worker.cancelWorkload(workloadId);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @SuppressWarnings({"PMD.UnusedLocalVariable"})
  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public StandardSyncOutput getReplicationOutput(final ReplicationActivityInput replicationActivityInput, final String workloadId) {
    final TracingContext tracingContext = buildTracingContext(replicationActivityInput);
    final Path jobRoot = TemporalUtils.getJobRoot(workspaceRoot, tracingContext.jobId, tracingContext.attemptNumber);

    try (final var mdcScope = new MdcScope.Builder().setExtraMdcEntries(LogSource.PLATFORM.toMdc()).build()) {
      logClientManager.setJobMdc(jobRoot);

      final var workerAndReplicationInput = getWorkerAndReplicationInput(replicationActivityInput);
      final WorkloadApiWorker worker = workerAndReplicationInput.worker;

      final ReplicationOutput output = worker.getOutput(workloadId);
      return finalizeOutput(replicationActivityInput, output);
    } catch (final Exception e) {
      ApmTraceUtils.addActualRootCauseToTrace(e);
      throw Activity.wrap(e);
    } finally {
      logClientManager.setJobMdc(null);
    }
  }

  private StandardSyncOutput finalizeOutput(final ReplicationActivityInput replicationActivityInput, final ReplicationOutput attemptOutput) {
    final TracingContext tracingContext = buildTracingContext(replicationActivityInput);
    ApmTraceUtils.addTagsToTrace(tracingContext.traceAttributes);

    final MetricAttribute[] metricAttributes = tracingContext.traceAttributes.entrySet().stream()
        .map(e -> new MetricAttribute(ApmTraceUtils.formatTag(e.getKey()), e.getValue().toString()))
        .collect(Collectors.toSet()).toArray(new MetricAttribute[] {});

    final StandardSyncOutput standardSyncOutput = reduceReplicationOutput(attemptOutput, metricAttributes);

    final String standardSyncOutputString = standardSyncOutput.toString();
    LOGGER.debug("sync summary: {}", standardSyncOutputString);
    if (standardSyncOutputString.length() > MAX_TEMPORAL_MESSAGE_SIZE) {
      LOGGER.error("Sync output exceeds the max temporal message size of {}, actual is {}.", MAX_TEMPORAL_MESSAGE_SIZE,
          standardSyncOutputString.length());
    } else {
      LOGGER.debug("Sync summary length: {}", standardSyncOutputString.length());
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

    replicationInput = replicationInputMapper.toReplicationInput(replicationActivityInput);
    worker = new WorkloadApiWorker(workloadOutputWriter, workloadApiClient,
        workloadClient, workloadIdGenerator, replicationActivityInput, featureFlagClient, logClientManager, dataplaneGroupResolver);

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
    if (replicationSummary.getStartTime() != null && replicationSummary.getEndTime() != null && replicationSummary.getBytesSynced() != null) {
      final var elapsedMs = replicationSummary.getEndTime() - replicationSummary.getStartTime();
      if (elapsedMs > 0) {
        final var elapsedSeconds = elapsedMs / 1000;
        final var throughput = replicationSummary.getBytesSynced() / elapsedSeconds;
        metricClient.count(OssMetricsRegistry.REPLICATION_THROUGHPUT_BPS, throughput, metricAttributes);
      }
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
