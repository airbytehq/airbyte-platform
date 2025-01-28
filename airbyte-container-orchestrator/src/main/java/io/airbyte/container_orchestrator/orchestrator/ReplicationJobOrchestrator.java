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
import io.airbyte.config.Configs;
import io.airbyte.config.FailureReason;
import io.airbyte.config.ReplicationOutput;
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
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs replication worker.
 */
@Singleton
public class ReplicationJobOrchestrator {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final ReplicationInput replicationInput;
  private final Configs configs;
  private final JobRunConfig jobRunConfig;
  private final ReplicationWorkerFactory replicationWorkerFactory;
  private final WorkloadApiClient workloadApiClient;
  private final JobOutputDocStore jobOutputDocStore;
  private final String workloadId;

  public ReplicationJobOrchestrator(final ReplicationInput replicationInput,
                                    @Named("workloadId") final String workloadId,
                                    final Configs configs,
                                    final JobRunConfig jobRunConfig,
                                    final ReplicationWorkerFactory replicationWorkerFactory,
                                    final WorkloadApiClient workloadApiClient,
                                    final JobOutputDocStore jobOutputDocStore) {
    this.replicationInput = replicationInput;
    this.workloadId = workloadId;
    this.configs = configs;
    this.jobRunConfig = jobRunConfig;
    this.replicationWorkerFactory = replicationWorkerFactory;
    this.workloadApiClient = workloadApiClient;
    this.jobOutputDocStore = jobOutputDocStore;
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
    final var jobRoot = TemporalUtils.getJobRoot(configs.getWorkspaceRoot(),
        jobRunConfig.getJobId(), jobRunConfig.getAttemptId());

    final ReplicationOutput replicationOutput;
    replicationOutput = run(replicationWorker, replicationInput, jobRoot, workloadId);
    jobOutputDocStore.writeSyncOutput(workloadId, replicationOutput);
    updateStatusInWorkloadApi(replicationOutput, workloadId);

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
    try {
      return replicationWorker.run(replicationInput, jobRoot);
    } catch (final DestinationException e) {
      failWorkload(workloadId, Optional.of(FailureHelper.destinationFailure(e, jobId, attemptNumber)));
      throw e;
    } catch (final SourceException e) {
      failWorkload(workloadId, Optional.of(FailureHelper.sourceFailure(e, jobId, attemptNumber)));
      throw e;
    } catch (final WorkerException e) {
      failWorkload(workloadId, Optional.of(FailureHelper.platformFailure(e, jobId, attemptNumber)));
      throw e;
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

}
