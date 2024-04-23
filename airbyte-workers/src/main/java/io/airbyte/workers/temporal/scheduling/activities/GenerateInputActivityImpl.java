/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.DESTINATION_DOCKER_IMAGE_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ID_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.SOURCE_DOCKER_IMAGE_KEY;
import static io.airbyte.metrics.lib.MetricTags.ATTEMPT_NUMBER;
import static io.airbyte.metrics.lib.MetricTags.CONNECTION_ID;
import static io.airbyte.metrics.lib.MetricTags.JOB_ID;

import datadog.trace.api.Trace;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.JobsApi;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.temporal.config.WorkerMode;
import io.airbyte.commons.temporal.utils.PayloadChecker;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.metrics.lib.MetricAttribute;
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

  private final PayloadChecker payloadChecker;

  @SuppressWarnings("ParameterName")
  public GenerateInputActivityImpl(final JobsApi jobsApi, final PayloadChecker payloadChecker) {
    this.jobsApi = jobsApi;
    this.payloadChecker = payloadChecker;
  }

  @Override
  public SyncJobCheckConnectionInputs getCheckConnectionInputs(final SyncInputWithAttemptNumber input) {
    return payloadChecker.validatePayloadSize(Jsons.convertValue(AirbyteApiClient.retryWithJitter(
        () -> jobsApi.getCheckInput(new io.airbyte.api.client.model.generated.CheckInput().jobId(input.getJobId())
            .attemptNumber(input.getAttemptNumber())),
        "Create check job input."), SyncJobCheckConnectionInputs.class));
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public JobInput getSyncWorkflowInput(final SyncInput input) {
    final var jobInput = Jsons.convertValue(AirbyteApiClient.retryWithJitter(
        () -> jobsApi.getJobInput(new io.airbyte.api.client.model.generated.SyncInput().jobId(input.getJobId())
            .attemptNumber(input.getAttemptId())),
        "Create job input."), JobInput.class);

    MetricAttribute[] attrs = new MetricAttribute[0];
    try {
      attrs = new MetricAttribute[] {
        new MetricAttribute(ApmTraceUtils.formatTag(CONNECTION_ID), jobInput.getDestinationLauncherConfig().getConnectionId().toString()),
        new MetricAttribute(ApmTraceUtils.formatTag(JOB_ID), jobInput.getJobRunConfig().getJobId()),
        new MetricAttribute(ApmTraceUtils.formatTag(ATTEMPT_NUMBER), jobInput.getJobRunConfig().getAttemptId().toString()),
        new MetricAttribute(ApmTraceUtils.formatTag(DESTINATION_DOCKER_IMAGE_KEY), jobInput.getDestinationLauncherConfig().getDockerImage()),
        new MetricAttribute(ApmTraceUtils.formatTag(SOURCE_DOCKER_IMAGE_KEY), jobInput.getSourceLauncherConfig().getDockerImage()),
      };
    } catch (final NullPointerException e) {
      // This shouldn't happen, but for good measure we're catching, because I don't want to introduce an
      // NPE in the critical path.
    }

    return payloadChecker.validatePayloadSize(jobInput, attrs);
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
