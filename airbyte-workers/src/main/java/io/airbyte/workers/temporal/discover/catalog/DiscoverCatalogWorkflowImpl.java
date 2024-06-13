/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.discover.catalog;

import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.ATTEMPT_NUMBER_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.DOCKER_IMAGE_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ID_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.WORKFLOW_TRACE_OPERATION_NAME;

import datadog.trace.api.Trace;
import io.airbyte.commons.temporal.annotations.TemporalActivityStub;
import io.airbyte.commons.temporal.scheduling.DiscoverCatalogWorkflow;
import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.config.StandardDiscoverCatalogInput;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.models.DiscoverCatalogInput;
import io.temporal.workflow.Workflow;
import java.util.Map;
import java.util.UUID;

/**
 * DiscoverCatalogWorkflowImpl.
 */
public class DiscoverCatalogWorkflowImpl implements DiscoverCatalogWorkflow {

  private static final String USE_WORKLOAD_API_FF_CHECK_TAG = "use_workload_api_ff_check";
  private static final int USE_WORKLOAD_API_FF_CHECK_VERSION = 1;

  @TemporalActivityStub(activityOptionsBeanName = "discoveryActivityOptions")
  private DiscoverCatalogActivity activity;

  @Trace(operationName = WORKFLOW_TRACE_OPERATION_NAME)
  @Override
  public ConnectorJobOutput run(final JobRunConfig jobRunConfig,
                                final IntegrationLauncherConfig launcherConfig,
                                final StandardDiscoverCatalogInput config) {
    ApmTraceUtils.addTagsToTrace(Map.of(ATTEMPT_NUMBER_KEY, jobRunConfig.getAttemptId(), JOB_ID_KEY, jobRunConfig.getJobId(), DOCKER_IMAGE_KEY,
        launcherConfig.getDockerImage()));
    final boolean shouldRunWithWorkload = checkUseWorkloadApiFlag(config.getActorContext().getWorkspaceId());
    ConnectorJobOutput result;
    if (shouldRunWithWorkload) {
      try {
        result = activity.runWithWorkload(new DiscoverCatalogInput(
            jobRunConfig, launcherConfig, config));
      } catch (WorkerException e) {
        activity.reportFailure(true);
        throw new RuntimeException(e);
      }
    } else {
      result = activity.run(jobRunConfig, launcherConfig, config);
    }

    if (result.getDiscoverCatalogId() != null) {
      activity.reportSuccess(shouldRunWithWorkload);
    } else {
      activity.reportFailure(shouldRunWithWorkload);
    }

    return result;
  }

  private boolean checkUseWorkloadApiFlag(final UUID workspaceId) {
    final int version = Workflow.getVersion(USE_WORKLOAD_API_FF_CHECK_TAG, Workflow.DEFAULT_VERSION, USE_WORKLOAD_API_FF_CHECK_VERSION);
    final boolean shouldCheckFlag = version >= USE_WORKLOAD_API_FF_CHECK_VERSION;

    if (!shouldCheckFlag) {
      return false;
    }

    return activity.shouldUseWorkload(workspaceId);
  }

}
