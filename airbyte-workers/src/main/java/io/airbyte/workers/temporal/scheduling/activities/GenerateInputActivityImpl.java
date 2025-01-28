/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
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
import io.airbyte.api.client.model.generated.CheckInput;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.micronaut.EnvConstants;
import io.airbyte.commons.temporal.exception.RetryableException;
import io.airbyte.commons.temporal.utils.PayloadChecker;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.workers.models.JobInput;
import io.airbyte.workers.models.SyncJobCheckConnectionInputs;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpStatus;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.Map;
import org.openapitools.client.infrastructure.ClientException;

/**
 * Generate input for a workflow.
 */
@Singleton
@Requires(env = EnvConstants.CONTROL_PLANE)
@SuppressWarnings({"PMD.EmptyCatchBlock", "PMD.AvoidCatchingNPE"})
public class GenerateInputActivityImpl implements GenerateInputActivity {

  private final AirbyteApiClient airbyteApiClient;

  private final PayloadChecker payloadChecker;

  @SuppressWarnings("ParameterName")
  public GenerateInputActivityImpl(final AirbyteApiClient airbyteApiClient, final PayloadChecker payloadChecker) {
    this.airbyteApiClient = airbyteApiClient;
    this.payloadChecker = payloadChecker;
  }

  @Override
  @SuppressWarnings("UNCHECKED_CAST")
  public SyncJobCheckConnectionInputs getCheckConnectionInputs(final SyncInputWithAttemptNumber input) {
    try {
      final var checkInput = airbyteApiClient.getJobsApi()
          .getCheckInput(new CheckInput(input.getJobId(), input.getAttemptNumber()));
      return payloadChecker.validatePayloadSize(
          Jsons.convertValue(checkInput, SyncJobCheckConnectionInputs.class));
    } catch (final ClientException e) {
      if (e.getStatusCode() == HttpStatus.NOT_FOUND.getCode()) {
        throw e;
      }
      throw new RetryableException(e);
    } catch (final IOException e) {
      throw new RetryableException(e);
    }
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  @SuppressWarnings("UNCHECKED_CAST")
  public JobInput getSyncWorkflowInput(final SyncInput input) throws IOException {
    final var jobInputResult =
        airbyteApiClient.getJobsApi().getJobInput(new io.airbyte.api.client.model.generated.SyncInput(input.getJobId(), input.getAttemptId()));
    final var jobInput = Jsons.convertValue(jobInputResult, JobInput.class);

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
  public JobInput getSyncWorkflowInputWithAttemptNumber(final SyncInputWithAttemptNumber input) throws IOException {
    ApmTraceUtils.addTagsToTrace(Map.of(JOB_ID_KEY, input.getJobId()));
    return getSyncWorkflowInput(new SyncInput(
        input.getAttemptNumber(),
        input.getJobId()));
  }

}
