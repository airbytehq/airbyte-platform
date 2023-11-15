/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.sync;

import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ReplicationOutput;
import io.airbyte.persistence.job.models.ReplicationInput;
import io.airbyte.workers.Worker;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.models.ReplicationActivityInput;
import io.airbyte.workers.orchestrator.OrchestratorNameGenerator;
import io.airbyte.workers.storage.DocumentStoreClient;
import io.airbyte.workers.workload.WorkloadIdGenerator;
import io.airbyte.workers.workload.WorkloadType;
import io.airbyte.workload.api.client2.generated.WorkloadApi;
import io.airbyte.workload.api.client2.model.generated.Workload;
import io.airbyte.workload.api.client2.model.generated.WorkloadCancelRequest;
import io.airbyte.workload.api.client2.model.generated.WorkloadCreateRequest;
import io.airbyte.workload.api.client2.model.generated.WorkloadLabel;
import io.airbyte.workload.api.client2.model.generated.WorkloadListResponse;
import io.airbyte.workload.api.client2.model.generated.WorkloadStatus;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Worker implementation that uses workload API instead of starting kube pods directly.
 */
public class WorkloadApiWorker implements Worker<ReplicationInput, ReplicationOutput> {

  private final static Logger log = LoggerFactory.getLogger(WorkloadApiWorker.class);
  private final static Set<WorkloadStatus> TERMINAL_STATUSES = Set.of(WorkloadStatus.cANCELLED, WorkloadStatus.fAILURE, WorkloadStatus.sUCCESS);
  private final DocumentStoreClient documentStoreClient;
  private final OrchestratorNameGenerator orchestratorNameGenerator;
  private final WorkloadApi workloadApi;
  private final WorkloadIdGenerator workloadIdGenerator;
  private final ReplicationActivityInput input;

  public WorkloadApiWorker(final DocumentStoreClient documentStoreClient,
                           final OrchestratorNameGenerator orchestratorNameGenerator,
                           final WorkloadApi workloadApi,
                           final WorkloadIdGenerator workloadIdGenerator,
                           final ReplicationActivityInput input) {
    this.documentStoreClient = documentStoreClient;
    this.orchestratorNameGenerator = orchestratorNameGenerator;
    this.workloadApi = workloadApi;
    this.workloadIdGenerator = workloadIdGenerator;
    this.input = input;
  }

  @Override
  public ReplicationOutput run(final ReplicationInput replicationInput, final Path jobRoot) throws WorkerException {
    final String serializedInput = Jsons.serialize(input);
    final String workloadId = workloadIdGenerator.generate(replicationInput.getConnectionId(),
        Long.parseLong(replicationInput.getJobRunConfig().getJobId()),
        replicationInput.getJobRunConfig().getAttemptId().intValue(),
        WorkloadType.SYNC);

    log.info("Creating workload {}", workloadId);

    // TODO worker may resume, check if job exists first
    // Create the workload
    createWorkload(new WorkloadCreateRequest(
        workloadId,
        List.of(
            new WorkloadLabel("connectionId", replicationInput.getConnectionId().toString()),
            new WorkloadLabel("jobId", replicationInput.getJobRunConfig().getJobId()),
            new WorkloadLabel("attemptNumber", replicationInput.getJobRunConfig().getAttemptId().toString())),
        serializedInput,
        jobRoot.toString()));

    // Wait until workload reaches a terminal status
    int i = 0;
    while (true) {
      final Workload workload = getWorkload(workloadId);

      if (workload.getStatus() != null) {
        if (TERMINAL_STATUSES.contains(workload.getStatus())) {
          break;
        }

        if (i++ % 5 == 0) {
          log.info("Workload {} is {}", workloadId, workload.getStatus());
        }
      }
      sleep(Duration.ofMinutes(1).toMillis());
    }

    return getReplicationOutput();
  }

  @Override
  public void cancel() {
    try {
      // TODO retry with jitter
      workloadApi.workloadCancel(new WorkloadCancelRequest("", "o//", "wl-api-worker"));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private ReplicationOutput getReplicationOutput() {
    final String outputLocation = orchestratorNameGenerator.getOrchestratorOutputLocation(input.getJobRunConfig().getJobId(),
        input.getJobRunConfig().getAttemptId());
    final Optional<String> output = documentStoreClient.read(outputLocation);
    return output.map(s -> Jsons.deserialize(s, ReplicationOutput.class)).orElse(null);
  }

  private WorkloadListResponse createWorkload(final WorkloadCreateRequest workloadCreateRequest) {
    try {
      return workloadApi.workloadCreate(workloadCreateRequest);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Workload getWorkload(final String workloadId) {
    try {
      return workloadApi.workloadGet(workloadId);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

}
