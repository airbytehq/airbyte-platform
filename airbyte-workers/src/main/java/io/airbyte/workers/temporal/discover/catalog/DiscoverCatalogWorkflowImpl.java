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
import java.util.Map;

/**
 * DiscoverCatalogWorkflowImpl.
 */
public class DiscoverCatalogWorkflowImpl implements DiscoverCatalogWorkflow {

  @TemporalActivityStub(activityOptionsBeanName = "discoveryActivityOptions")
  private DiscoverCatalogActivity activity;
  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private DiscoverCatalogHelperActivity reportActivity;

  @Trace(operationName = WORKFLOW_TRACE_OPERATION_NAME)
  @Override
  public ConnectorJobOutput run(final JobRunConfig jobRunConfig,
                                final IntegrationLauncherConfig launcherConfig,
                                final StandardDiscoverCatalogInput config) {
    ApmTraceUtils.addTagsToTrace(Map.of(ATTEMPT_NUMBER_KEY, jobRunConfig.getAttemptId(), JOB_ID_KEY, jobRunConfig.getJobId(), DOCKER_IMAGE_KEY,
        launcherConfig.getDockerImage()));
    ConnectorJobOutput result;
    try {
      result = activity.runWithWorkload(new DiscoverCatalogInput(
          jobRunConfig, launcherConfig, config));
    } catch (WorkerException e) {
      reportActivity.reportFailure();
      throw new RuntimeException(e);
    }

    if (result.getDiscoverCatalogId() != null) {
      reportActivity.reportSuccess();
    } else {
      reportActivity.reportFailure();
    }

    return result;
  }

}
