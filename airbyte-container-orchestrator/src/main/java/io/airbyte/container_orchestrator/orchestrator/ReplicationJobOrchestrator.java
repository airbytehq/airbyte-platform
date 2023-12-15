/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
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
import io.airbyte.container_orchestrator.AsyncStateManager;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.persistence.job.models.ReplicationInput;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.general.ReplicationWorker;
import io.airbyte.workers.general.ReplicationWorkerFactory;
import io.airbyte.workers.helper.FailureHelper;
import io.airbyte.workers.internal.exception.DestinationException;
import io.airbyte.workers.internal.exception.SourceException;
import io.airbyte.workers.process.AsyncKubePodStatus;
import io.airbyte.workers.process.KubePodProcess;
import io.airbyte.workers.sync.ReplicationLauncherWorker;
import io.airbyte.workers.workload.JobOutputDocStore;
import io.airbyte.workers.workload.WorkloadIdGenerator;
import io.airbyte.workload.api.client.generated.WorkloadApi;
import io.airbyte.workload.api.client.model.generated.WorkloadCancelRequest;
import io.airbyte.workload.api.client.model.generated.WorkloadFailureRequest;
import io.airbyte.workload.api.client.model.generated.WorkloadSuccessRequest;
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
public class ReplicationJobOrchestrator implements JobOrchestrator<ReplicationInput> {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final Configs configs;
  private final JobRunConfig jobRunConfig;
  private final ReplicationWorkerFactory replicationWorkerFactory;
  // Used by the orchestrator to mark the job RUNNING once the relevant pods are spun up.
  private final AsyncStateManager asyncStateManager;
  private final WorkloadApi workloadApi;
  private final WorkloadIdGenerator workloadIdGenerator;
  private final boolean workloadEnabled;
  private final JobOutputDocStore jobOutputDocStore;

  public ReplicationJobOrchestrator(final Configs configs,
                                    final JobRunConfig jobRunConfig,
                                    final ReplicationWorkerFactory replicationWorkerFactory,
                                    final AsyncStateManager asyncStateManager,
                                    final WorkloadApi workloadApi,
                                    final WorkloadIdGenerator workloadIdGenerator,
                                    final boolean workloadEnabled,
                                    final JobOutputDocStore jobOutputDocStore) {
    this.configs = configs;
    this.jobRunConfig = jobRunConfig;
    this.replicationWorkerFactory = replicationWorkerFactory;
    this.asyncStateManager = asyncStateManager;
    this.workloadApi = workloadApi;
    this.workloadIdGenerator = workloadIdGenerator;
    this.workloadEnabled = workloadEnabled;
    this.jobOutputDocStore = jobOutputDocStore;
  }

  @Override
  public String getOrchestratorName() {
    return "Replication";
  }

  @Override
  public Class<ReplicationInput> getInputClass() {
    return ReplicationInput.class;
  }

  @Trace(operationName = JOB_ORCHESTRATOR_OPERATION_NAME)
  @Override
  public Optional<String> runJob() throws Exception {
    final var replicationInput = readInput();

    final var sourceLauncherConfig = JobOrchestrator.readAndDeserializeFile(
        Path.of(KubePodProcess.CONFIG_DIR, ReplicationLauncherWorker.INIT_FILE_SOURCE_LAUNCHER_CONFIG),
        IntegrationLauncherConfig.class);

    final var destinationLauncherConfig = JobOrchestrator.readAndDeserializeFile(
        Path.of(KubePodProcess.CONFIG_DIR, ReplicationLauncherWorker.INIT_FILE_DESTINATION_LAUNCHER_CONFIG),
        IntegrationLauncherConfig.class);
    log.info("sourceLauncherConfig is: " + sourceLauncherConfig.toString());

    ApmTraceUtils.addTagsToTrace(
        Map.of(JOB_ID_KEY, jobRunConfig.getJobId(),
            DESTINATION_DOCKER_IMAGE_KEY, destinationLauncherConfig.getDockerImage(),
            SOURCE_DOCKER_IMAGE_KEY, sourceLauncherConfig.getDockerImage()));

    final ReplicationWorker replicationWorker =
        replicationWorkerFactory.create(replicationInput, jobRunConfig, sourceLauncherConfig, destinationLauncherConfig, this::markJobRunning);

    log.info("Running replication worker...");
    final var jobRoot = TemporalUtils.getJobRoot(configs.getWorkspaceRoot(),
        jobRunConfig.getJobId(), jobRunConfig.getAttemptId());

    final ReplicationOutput replicationOutput;
    if (workloadEnabled) {
      replicationOutput = runWithWorkloadEnabled(replicationWorker, replicationInput, jobRoot);
      final String workloadId = workloadIdGenerator.generateSyncWorkloadId(
          replicationInput.getConnectionId(),
          Long.parseLong(jobRunConfig.getJobId()),
          Math.toIntExact(jobRunConfig.getAttemptId()));
      jobOutputDocStore.writeSyncOutput(workloadId, replicationOutput);
    } else {
      replicationOutput = replicationWorker.run(replicationInput, jobRoot);
    }

    log.info("Returning output...");
    return Optional.of(Jsons.serialize(replicationOutput));
  }

  @VisibleForTesting
  ReplicationOutput runWithWorkloadEnabled(final ReplicationWorker replicationWorker, final ReplicationInput replicationInput, final Path jobRoot)
      throws WorkerException, IOException {

    final Long jobId = Long.parseLong(jobRunConfig.getJobId());
    final Integer attemptNumber = Math.toIntExact(jobRunConfig.getAttemptId());
    final String workloadId = workloadIdGenerator.generateSyncWorkloadId(
        replicationInput.getConnectionId(),
        jobId,
        attemptNumber);

    try {
      final ReplicationOutput replicationOutput = replicationWorker.run(replicationInput, jobRoot);
      switch (replicationOutput.getReplicationAttemptSummary().getStatus()) {
        case FAILED -> failWorkload(workloadId, replicationOutput.getFailures().stream().findFirst());
        case CANCELLED -> cancelWorkload(workloadId);
        case COMPLETED -> succeedWorkload(workloadId);
        default -> throw new RuntimeException(String.format("Unknown status %s.", replicationOutput.getReplicationAttemptSummary().getStatus()));
      }

      return replicationOutput;
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

  private void cancelWorkload(final String workloadId) throws IOException {
    workloadApi.workloadCancel(new WorkloadCancelRequest(workloadId, "Replication job has been cancelled", "orchestrator"));
  }

  private void failWorkload(final String workloadId, final Optional<FailureReason> failureReason) throws IOException {
    if (failureReason.isPresent()) {
      workloadApi.workloadFailure(new WorkloadFailureRequest(workloadId,
          failureReason.get().getFailureOrigin().value(),
          failureReason.get().getExternalMessage()));
    } else {
      workloadApi.workloadFailure(new WorkloadFailureRequest(workloadId, null, null));
    }
  }

  private void succeedWorkload(final String workloadId) throws IOException {
    workloadApi.workloadSuccess(new WorkloadSuccessRequest(workloadId));
  }

  private void markJobRunning() {
    asyncStateManager.write(AsyncKubePodStatus.RUNNING);
  }

}
