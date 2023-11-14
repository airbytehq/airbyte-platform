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
import io.airbyte.workers.workload.WorkloadIdGenerator;
import io.airbyte.workers.workload.WorkloadType;
import io.airbyte.workload.api.client2.generated.WorkloadApi;
import io.airbyte.workload.api.client2.model.generated.WorkloadCancelRequest;
import io.airbyte.workload.api.client2.model.generated.WorkloadCreateRequest;
import io.airbyte.workload.api.client2.model.generated.WorkloadLabel;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class WorkloadApiWorker implements Worker<ReplicationInput, ReplicationOutput> {

  private final WorkloadApi workloadApi;
  private final WorkloadIdGenerator workloadIdGenerator;
  private final ReplicationActivityInput input;

  public WorkloadApiWorker(final WorkloadApi workloadApi, final WorkloadIdGenerator workloadIdGenerator, final ReplicationActivityInput input) {
    this.workloadApi = workloadApi;
    this.workloadIdGenerator = workloadIdGenerator;
    this.input = input;
  }

  @Override
  public ReplicationOutput run(final ReplicationInput replicationInput, Path jobRoot) throws WorkerException {
    try {
      final String serializedInput = Jsons.serialize(input);

      // TODO worker may resume, check if job exists first

      // TODO Retry with jitter
      workloadApi.workloadCreate(
          new WorkloadCreateRequest(
              workloadIdGenerator.generate(replicationInput.getConnectionId(),
                  Long.parseLong(replicationInput.getJobRunConfig().getJobId()),
                  replicationInput.getJobRunConfig().getAttemptId().intValue(),
                  WorkloadType.SYNC),
              serializedInput,
              List.of(
                  new WorkloadLabel("connectionId", replicationInput.getConnectionId().toString()),
                  new WorkloadLabel("jobId", replicationInput.getJobRunConfig().getJobId().toString()),
                  new WorkloadLabel("attemptNumber", replicationInput.getJobRunConfig().getAttemptId().toString()))));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // TODO add polling for results loop

    // TODO read the output
    return null;
  }

  @Override
  public void cancel() {
    try {
      // TODO retry with jitter
      workloadApi.workloadCancel(new WorkloadCancelRequest(List.of(""), "o//", "wl-api-worker"));
    } catch (IOException e) {
      // TODO re-enable exception catching once cancel is implemented
      // throw new RuntimeException(e);
    }
  }

}
