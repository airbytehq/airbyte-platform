/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container_orchestrator.orchestrator;

import static io.airbyte.metrics.lib.ApmTraceConstants.JOB_ORCHESTRATOR_OPERATION_NAME;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.DESTINATION_DOCKER_IMAGE_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ID_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.SOURCE_DOCKER_IMAGE_KEY;

import com.google.common.annotations.VisibleForTesting;
import datadog.trace.api.Trace;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.temporal.TemporalUtils;
import io.airbyte.commons.version.AirbyteVersion;
import io.airbyte.config.FailureReason;
import io.airbyte.config.ReplicationAttemptSummary;
import io.airbyte.config.ReplicationOutput;
import io.airbyte.metrics.MetricAttribute;
import io.airbyte.metrics.MetricClient;
import io.airbyte.metrics.OssMetricsRegistry;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.persistence.job.models.ReplicationInput;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.general.BufferedReplicationWorker;
import io.airbyte.workers.general.ReplicationWorkerFactory;
import io.airbyte.workers.helper.FailureHelper;
import io.airbyte.workers.internal.exception.DestinationException;
import io.airbyte.workers.internal.exception.SourceException;
import io.airbyte.workers.workload.JobOutputDocStore;
import io.airbyte.workload.api.client.WorkloadApiClient;
import io.airbyte.workload.api.client.model.generated.WorkloadCancelRequest;
import io.airbyte.workload.api.client.model.generated.WorkloadFailureRequest;
import io.airbyte.workload.api.client.model.generated.WorkloadSuccessRequest;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.ToDoubleFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs replication worker.
 */
@Singleton
public class ReplicationJobOrchestrator {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @VisibleForTesting
  public static final double BYTES_TO_GB = 1024 * 1024 * 1024;
  @VisibleForTesting
  public static final String STATUS_ATTRIBUTE = "status";
  @VisibleForTesting
  public static final String FAILED_STATUS = "failed";
  @VisibleForTesting
  public static final String SUCCESS_STATUS = "success";

  private static final ToDoubleFunction<ReplicationAttemptSummary> BYTES_GAUGE_FUNCTION =
      (replicationAttemptSummary) -> replicationAttemptSummary.getBytesSynced() / BYTES_TO_GB;
  private static final ToDoubleFunction<ReplicationAttemptSummary> DURATION_GAUGE_FUNCTION =
      (replicationAttemptSummary) -> Duration.ofMillis(replicationAttemptSummary.getEndTime() - replicationAttemptSummary.getStartTime()).toSeconds();

  private final ReplicationInput replicationInput;
  private final Path workspaceRoot;
  private final JobRunConfig jobRunConfig;
  private final ReplicationWorkerFactory replicationWorkerFactory;
  private final WorkloadApiClient workloadApiClient;
  private final JobOutputDocStore jobOutputDocStore;
  private final String workloadId;
  private final MetricClient metricClient;
  private final AirbyteVersion airbyteVersion;

  public ReplicationJobOrchestrator(final ReplicationInput replicationInput,
                                    @Named("workloadId") final String workloadId,
                                    @Named("workspaceRoot") final Path workspaceRoot,
                                    final JobRunConfig jobRunConfig,
                                    final ReplicationWorkerFactory replicationWorkerFactory,
                                    final WorkloadApiClient workloadApiClient,
                                    final JobOutputDocStore jobOutputDocStore,
                                    final MetricClient metricClient,
                                    final AirbyteVersion airbyteVersion) {
    this.replicationInput = replicationInput;
    this.workloadId = workloadId;
    this.workspaceRoot = workspaceRoot;
    this.jobRunConfig = jobRunConfig;
    this.replicationWorkerFactory = replicationWorkerFactory;
    this.workloadApiClient = workloadApiClient;
    this.jobOutputDocStore = jobOutputDocStore;
    this.metricClient = metricClient;
    this.airbyteVersion = airbyteVersion;
  }

  @Trace(operationName = JOB_ORCHESTRATOR_OPERATION_NAME)
  public Optional<String> runJob() throws Exception {

    final var sourceLauncherConfig = replicationInput.getSourceLauncherConfig();

    final var destinationLauncherConfig = replicationInput.getDestinationLauncherConfig();

    ApmTraceUtils.addTagsToTrace(
        Map.of(JOB_ID_KEY, jobRunConfig.getJobId(),
            DESTINATION_DOCKER_IMAGE_KEY, destinationLauncherConfig.getDockerImage(),
            SOURCE_DOCKER_IMAGE_KEY, sourceLauncherConfig.getDockerImage()));
    final BufferedReplicationWorker replicationWorker =
        replicationWorkerFactory.create(replicationInput, jobRunConfig, sourceLauncherConfig, destinationLauncherConfig, () -> {},
            workloadId);

    log.info("Running replication worker...");
    final var jobRoot = TemporalUtils.getJobRoot(workspaceRoot,
        jobRunConfig.getJobId(), jobRunConfig.getAttemptId());

    final ReplicationOutput replicationOutput = run(replicationWorker, replicationInput, jobRoot, workloadId);
    jobOutputDocStore.writeSyncOutput(workloadId, replicationOutput);
    updateStatusInWorkloadApi(replicationOutput, workloadId);

    final List<MetricAttribute> attributes = buildMetricAttributes(replicationInput,
        Long.parseLong(jobRunConfig.getJobId()), jobRunConfig.getAttemptId().intValue());
    metricClient.gauge(OssMetricsRegistry.SYNC_DURATION,
        replicationOutput.getReplicationAttemptSummary(),
        DURATION_GAUGE_FUNCTION,
        attributes.toArray(new MetricAttribute[0]));
    metricClient.gauge(OssMetricsRegistry.SYNC_GB_MOVED,
        replicationOutput.getReplicationAttemptSummary(),
        BYTES_GAUGE_FUNCTION,
        attributes.toArray(new MetricAttribute[0]));

    log.info("Returning output...");
    return Optional.of(Jsons.serialize(replicationOutput));
  }

  @VisibleForTesting
  ReplicationOutput run(final BufferedReplicationWorker replicationWorker,
                        final ReplicationInput replicationInput,
                        final Path jobRoot,
                        final String workloadId)
      throws WorkerException, IOException {

    final Long jobId = Long.parseLong(jobRunConfig.getJobId());
    final Integer attemptNumber = Math.toIntExact(jobRunConfig.getAttemptId());
    final List<MetricAttribute> attributes = buildMetricAttributes(replicationInput, jobId, attemptNumber);

    try {
      return replicationWorker.run(replicationInput, jobRoot);
    } catch (final DestinationException e) {
      failWorkload(workloadId, Optional.of(FailureHelper.destinationFailure(e, jobId, attemptNumber)));
      attributes.add(new MetricAttribute(STATUS_ATTRIBUTE, FAILED_STATUS));
      throw e;
    } catch (final SourceException e) {
      failWorkload(workloadId, Optional.of(FailureHelper.sourceFailure(e, jobId, attemptNumber)));
      attributes.add(new MetricAttribute(STATUS_ATTRIBUTE, FAILED_STATUS));
      throw e;
    } catch (final WorkerException e) {
      failWorkload(workloadId, Optional.of(FailureHelper.platformFailure(e, jobId, attemptNumber)));
      attributes.add(new MetricAttribute(STATUS_ATTRIBUTE, FAILED_STATUS));
      throw e;
    } finally {
      if (attributes.stream().noneMatch(a -> STATUS_ATTRIBUTE.equalsIgnoreCase(a.getKey()))) {
        attributes.add(new MetricAttribute(STATUS_ATTRIBUTE, SUCCESS_STATUS));
      }
      metricClient.count(OssMetricsRegistry.SYNC_STATUS, attributes.toArray(new MetricAttribute[0]));
    }
  }

  @VisibleForTesting
  void updateStatusInWorkloadApi(final ReplicationOutput replicationOutput, final String workloadId) throws IOException {
    if (replicationOutput == null || replicationOutput.getReplicationAttemptSummary() == null) {
      log.warn("The replication output is null, skipping updating the workload status via API");
      return;
    }
    switch (replicationOutput.getReplicationAttemptSummary().getStatus()) {
      case FAILED -> failWorkload(workloadId, replicationOutput.getFailures().stream().findFirst());
      case CANCELLED -> cancelWorkload(workloadId);
      case COMPLETED -> succeedWorkload(workloadId);
      default -> throw new RuntimeException(String.format("Unknown status %s.", replicationOutput.getReplicationAttemptSummary().getStatus()));
    }
  }

  private void cancelWorkload(final String workloadId) throws IOException {
    workloadApiClient.getWorkloadApi().workloadCancel(new WorkloadCancelRequest(workloadId, "Replication job has been cancelled", "orchestrator"));
  }

  private void failWorkload(final String workloadId, final Optional<FailureReason> failureReason) throws IOException {
    if (failureReason.isPresent()) {
      workloadApiClient.getWorkloadApi().workloadFailure(new WorkloadFailureRequest(workloadId,
          failureReason.get().getFailureOrigin().value(),
          failureReason.get().getExternalMessage()));
    } else {
      workloadApiClient.getWorkloadApi().workloadFailure(new WorkloadFailureRequest(workloadId, null, null));
    }
  }

  private void succeedWorkload(final String workloadId) throws IOException {
    workloadApiClient.getWorkloadApi().workloadSuccess(new WorkloadSuccessRequest(workloadId));
  }

  @VisibleForTesting
  public List<MetricAttribute> buildMetricAttributes(final ReplicationInput replicationInput, final Long jobId, final Integer attemptNumber) {
    final List<MetricAttribute> attributes = new ArrayList<>();
    attributes.add(new MetricAttribute("attempt_count", String.valueOf(attemptNumber + 1))); // Normalize to make it human understandable
    attributes.add(new MetricAttribute("connection_id", replicationInput.getConnectionId().toString()));
    attributes.add(new MetricAttribute("workspace_id", replicationInput.getWorkspaceId().toString()));
    attributes.add(new MetricAttribute("job_id", jobId.toString()));
    attributes.add(new MetricAttribute("destination_connector_id", replicationInput.getDestinationId().toString()));
    attributes.add(new MetricAttribute("source_connector_id", replicationInput.getSourceId().toString()));
    attributes.add(new MetricAttribute("version", airbyteVersion.serialize()));
    return attributes;
  }

}
