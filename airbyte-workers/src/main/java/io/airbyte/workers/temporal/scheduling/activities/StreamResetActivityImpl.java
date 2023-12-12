/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.CONNECTION_ID_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ID_KEY;

import datadog.trace.api.Trace;
import io.airbyte.api.client.generated.JobsApi;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.DeleteStreamResetRecordsForJobRequest;
import io.airbyte.commons.temporal.config.WorkerMode;
import io.airbyte.commons.temporal.exception.RetryableException;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * StreamResetActivityImpl.
 */
@Slf4j
@Singleton
@Requires(env = WorkerMode.CONTROL_PLANE)
public class StreamResetActivityImpl implements StreamResetActivity {

  private final JobsApi jobsApi;

  public StreamResetActivityImpl(final JobsApi jobsApi) {
    this.jobsApi = jobsApi;
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public void deleteStreamResetRecordsForJob(final DeleteStreamResetRecordsForJobInput input) {
    ApmTraceUtils.addTagsToTrace(Map.of(CONNECTION_ID_KEY, input.getConnectionId(), JOB_ID_KEY, input.getJobId()));

    try {
      jobsApi.deleteStreamResetRecordsForJob(new DeleteStreamResetRecordsForJobRequest()
          .connectionId(input.getConnectionId())
          .jobId(input.getJobId()));
    } catch (final ApiException e) {
      throw new RetryableException(e);
    }
  }

}
