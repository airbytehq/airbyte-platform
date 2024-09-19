/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.spec;

import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.ATTEMPT_NUMBER_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.DOCKER_IMAGE_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ID_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.WORKFLOW_TRACE_OPERATION_NAME;

import datadog.trace.api.Trace;
import io.airbyte.commons.temporal.annotations.TemporalActivityStub;
import io.airbyte.commons.temporal.scheduling.SpecWorkflow;
import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.models.SpecInput;
import java.util.Map;

/**
 * SpecWorkflowImpl.
 */
public class SpecWorkflowImpl implements SpecWorkflow {

  @TemporalActivityStub(activityOptionsBeanName = "specActivityOptions")
  private SpecActivity activity;

  @Trace(operationName = WORKFLOW_TRACE_OPERATION_NAME)
  @Override
  public ConnectorJobOutput run(final JobRunConfig jobRunConfig, final IntegrationLauncherConfig launcherConfig) {
    ApmTraceUtils.addTagsToTrace(Map.of(ATTEMPT_NUMBER_KEY, jobRunConfig.getAttemptId(), DOCKER_IMAGE_KEY, launcherConfig.getDockerImage(),
        JOB_ID_KEY, jobRunConfig.getJobId()));
    ConnectorJobOutput result;
    try {
      result = activity.runWithWorkload(new SpecInput(jobRunConfig, launcherConfig));
    } catch (WorkerException e) {
      activity.reportFailure();
      throw new RuntimeException(e);
    }

    if (result.getSpec() != null) {
      activity.reportSuccess();
    } else {
      activity.reportFailure();
    }

    return result;
  }

}
