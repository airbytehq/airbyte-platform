/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ID_KEY;

import datadog.trace.api.Trace;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.JobsApi;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.temporal.config.WorkerMode;
import io.airbyte.commons.temporal.utils.PayloadChecker;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.workers.models.JobInput;
import io.airbyte.workers.models.SyncJobCheckConnectionInputs;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import java.util.Map;

/**
 * Generate input for a workflow.
 */
@Singleton
@Requires(env = WorkerMode.CONTROL_PLANE)
public class GenerateInputActivityImpl implements GenerateInputActivity {

  private final JobsApi jobsApi;

  @SuppressWarnings("ParameterName")
  public GenerateInputActivityImpl(final JobsApi jobsApi) {
    this.jobsApi = jobsApi;
  }

  @Override
  public SyncJobCheckConnectionInputs getCheckConnectionInputs(final SyncInputWithAttemptNumber input) {
    return PayloadChecker.validatePayloadSize(Jsons.convertValue(AirbyteApiClient.retryWithJitter(
        () -> jobsApi.getCheckInput(new io.airbyte.api.client.model.generated.CheckInput().jobId(input.getJobId())
            .attemptNumber(input.getAttemptNumber())),
        "Create check job input."), SyncJobCheckConnectionInputs.class));
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public JobInput getSyncWorkflowInput(final SyncInput input) {
    return PayloadChecker.validatePayloadSize(Jsons.convertValue(AirbyteApiClient.retryWithJitter(
        () -> jobsApi.getJobInput(new io.airbyte.api.client.model.generated.SyncInput().jobId(input.getJobId())
            .attemptNumber(input.getAttemptId())),
        "Create job input."), JobInput.class));
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public JobInput getSyncWorkflowInputWithAttemptNumber(final SyncInputWithAttemptNumber input) {
    ApmTraceUtils.addTagsToTrace(Map.of(JOB_ID_KEY, input.getJobId()));
    return getSyncWorkflowInput(new SyncInput(
        input.getAttemptNumber(),
        input.getJobId()));
  }

}
