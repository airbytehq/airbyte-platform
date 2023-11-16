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
import io.airbyte.workload.api.client.generated.WorkloadApi;
import io.airbyte.workload.api.client.model.generated.Workload;
import io.airbyte.workload.api.client.model.generated.WorkloadCancelRequest;
import io.airbyte.workload.api.client.model.generated.WorkloadCreateRequest;
import io.airbyte.workload.api.client.model.generated.WorkloadLabel;
import io.airbyte.workload.api.client.model.generated.WorkloadStatus;
import io.micronaut.http.HttpStatus;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.openapitools.client.infrastructure.ClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Worker implementation that uses workload API instead of starting kube pods directly.
 */
public class WorkloadApiWorker implements Worker<ReplicationInput, ReplicationOutput> {

  private static final Logger log = LoggerFactory.getLogger(WorkloadApiWorker.class);
  private static final Set<WorkloadStatus> TERMINAL_STATUSES = Set.of(WorkloadStatus.CANCELLED, WorkloadStatus.FAILURE, WorkloadStatus.SUCCESS);
  private final DocumentStoreClient documentStoreClient;
  private final OrchestratorNameGenerator orchestratorNameGenerator;
  private final WorkloadApi workloadApi;
  private final WorkloadIdGenerator workloadIdGenerator;
  private final ReplicationActivityInput input;
  private String workloadId = null;

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
  @SuppressWarnings("PMD.AssignmentInOperand")
  public ReplicationOutput run(final ReplicationInput replicationInput, final Path jobRoot) throws WorkerException {
    final String serializedInput = Jsons.serialize(input);
    workloadId = workloadIdGenerator.generate(replicationInput.getConnectionId(),
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
          log.info("Workload {} has returned a terminal status of {}.  Fetching output...", workloadId, workload.getStatus());
          break;
        }

        if (i % 5 == 0) {
          log.info("Workload {} is {}", workloadId, workload.getStatus());
        }
        i++;
      }
      sleep(Duration.ofMinutes(1).toMillis());
    }

    return getReplicationOutput(workloadId);
  }

  @Override
  public void cancel() {
    try {
      if (workloadId != null) {
        workloadApi.workloadCancel(new WorkloadCancelRequest(workloadId, "user requested", "WorkloadApiWorker"));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private ReplicationOutput getReplicationOutput(final String workloadId) {
    final String outputLocation = orchestratorNameGenerator.getOrchestratorOutputLocation(input.getJobRunConfig().getJobId(),
        input.getJobRunConfig().getAttemptId());
    final Optional<String> output = documentStoreClient.read(outputLocation);
    log.info("Replication output for workload {} : {}", workloadId, output.orElse(""));
    return output.map(s -> Jsons.deserialize(s, ReplicationOutput.class)).orElse(null);
  }

  private void createWorkload(final WorkloadCreateRequest workloadCreateRequest) {
    try {
      workloadApi.workloadCreate(workloadCreateRequest);
    } catch (final ClientException e) {
      /*
       * The Workload API returns a 304 response when the request to execute the workload has already been
       * created. That response is handled in the form of a ClientException by the generated OpenAPI
       * client. We do not want to cause the Temporal workflow to retry, so catch it and log the
       * information so that the workflow will continue.
       */
      if (e.getStatusCode() == HttpStatus.NOT_MODIFIED.getCode()) {
        log.warn("Workload {} already created and in progress.  Continuing...", workloadCreateRequest.getWorkloadId());
      } else {
        throw new RuntimeException(e);
      }
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Workload getWorkload(final String workloadId) {
    try {
      return workloadApi.workloadGet(workloadId);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void sleep(final long millis) {
    try {
      Thread.sleep(millis);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

}
