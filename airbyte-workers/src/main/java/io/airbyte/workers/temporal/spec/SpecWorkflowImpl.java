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
import io.temporal.workflow.Workflow;
import java.util.Map;
import java.util.UUID;

/**
 * SpecWorkflowImpl.
 */
public class SpecWorkflowImpl implements SpecWorkflow {

  private static final String USE_WORKLOAD_API_FF_CHECK_TAG = "use_workload_api_ff_check";
  private static final int USE_WORKLOAD_API_FF_CHECK_VERSION = 1;

  @TemporalActivityStub(activityOptionsBeanName = "specActivityOptions")
  private SpecActivity activity;

  @Trace(operationName = WORKFLOW_TRACE_OPERATION_NAME)
  @Override
  public ConnectorJobOutput run(final JobRunConfig jobRunConfig, final IntegrationLauncherConfig launcherConfig) {
    ApmTraceUtils.addTagsToTrace(Map.of(ATTEMPT_NUMBER_KEY, jobRunConfig.getAttemptId(), DOCKER_IMAGE_KEY, launcherConfig.getDockerImage(),
        JOB_ID_KEY, jobRunConfig.getJobId()));
    boolean shouldRunWithWorkload = checkUseWorkloadApiFlag(launcherConfig.getWorkspaceId());
    ConnectorJobOutput result;
    if (shouldRunWithWorkload) {
      try {
        result = activity.runWithWorkload(new SpecInput(jobRunConfig, launcherConfig));
      } catch (WorkerException e) {
        activity.reportFailure();
        throw new RuntimeException(e);
      }
    } else {
      result = activity.run(jobRunConfig, launcherConfig);
    }

    if (result.getSpec() != null) {
      activity.reportSuccess();
    } else {
      activity.reportFailure();
    }

    return result;
  }

  private boolean checkUseWorkloadApiFlag(final UUID workspaceId) {
    final int version = Workflow.getVersion(USE_WORKLOAD_API_FF_CHECK_TAG, Workflow.DEFAULT_VERSION, USE_WORKLOAD_API_FF_CHECK_VERSION);
    final boolean shouldCheckFlag = version >= USE_WORKLOAD_API_FF_CHECK_VERSION;

    if (!shouldCheckFlag) {
      return false;
    }

    if (workspaceId == null) {
      return activity.shouldUseWorkload(UUID.randomUUID());
    } else {
      return activity.shouldUseWorkload(workspaceId);
    }
  }

}
