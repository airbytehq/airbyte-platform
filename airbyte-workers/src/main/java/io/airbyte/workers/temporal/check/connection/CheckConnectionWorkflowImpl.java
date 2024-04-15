/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.check.connection;

import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.ATTEMPT_NUMBER_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.DOCKER_IMAGE_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ID_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.WORKFLOW_TRACE_OPERATION_NAME;

import datadog.trace.api.Trace;
import io.airbyte.commons.temporal.annotations.TemporalActivityStub;
import io.airbyte.commons.temporal.scheduling.CheckConnectionWorkflow;
import io.airbyte.commons.temporal.utils.ActivityFailureClassifier;
import io.airbyte.config.ActorType;
import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.config.ConnectorJobOutput.OutputType;
import io.airbyte.config.FailureReason;
import io.airbyte.config.FailureReason.FailureOrigin;
import io.airbyte.config.StandardCheckConnectionInput;
import io.airbyte.config.StandardCheckConnectionOutput;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.workers.models.CheckConnectionInput;
import io.temporal.workflow.Workflow;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.exception.ExceptionUtils;

/**
 * Check connection temporal workflow implementation.
 */
public class CheckConnectionWorkflowImpl implements CheckConnectionWorkflow {

  private static final String USE_WORKLOAD_API_FF_CHECK_TAG = "use_workload_api_ff_check";
  private static final int USE_WORKLOAD_API_FF_CHECK_VERSION = 1;

  @TemporalActivityStub(activityOptionsBeanName = "checkActivityOptions")
  private CheckConnectionActivity activity;

  @Trace(operationName = WORKFLOW_TRACE_OPERATION_NAME)
  @Override
  public ConnectorJobOutput run(final JobRunConfig jobRunConfig,
                                final IntegrationLauncherConfig launcherConfig,
                                final StandardCheckConnectionInput connectionConfiguration) {
    ApmTraceUtils.addTagsToTrace(Map.of(ATTEMPT_NUMBER_KEY, jobRunConfig.getAttemptId(), JOB_ID_KEY, jobRunConfig.getJobId(), DOCKER_IMAGE_KEY,
        launcherConfig.getDockerImage()));
    ConnectorJobOutput result;

    try {
      final CheckConnectionInput checkInput = new CheckConnectionInput(jobRunConfig, launcherConfig, connectionConfiguration);
      boolean shouldRunWithWorkload = checkUseWorkloadApiFlag(connectionConfiguration.getActorContext().getWorkspaceId());

      if (shouldRunWithWorkload) {
        result = activity.runWithWorkload(checkInput);
      } else {
        result = activity.runWithJobOutput(checkInput);
      }
    } catch (final Exception e) {
      result = new ConnectorJobOutput().withOutputType(OutputType.CHECK_CONNECTION)
          .withCheckConnection(
              new StandardCheckConnectionOutput()
                  .withStatus(StandardCheckConnectionOutput.Status.FAILED)
                  .withMessage("The check connection failed."))
          .withFailureReason(getFailureReason(connectionConfiguration, e));
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

  private FailureReason getFailureReason(final StandardCheckConnectionInput connectionConfiguration, final Exception e) {
    final FailureReason failureReason = new FailureReason()
        .withFailureOrigin(
            connectionConfiguration.getActorType() == ActorType.SOURCE ? FailureReason.FailureOrigin.SOURCE : FailureReason.FailureOrigin.DESTINATION)
        .withStacktrace(ExceptionUtils.getStackTrace(e));
    switch (ActivityFailureClassifier.classifyException(e)) {
      case HEARTBEAT:
        failureReason
            .withFailureOrigin(FailureOrigin.AIRBYTE_PLATFORM)
            .withExternalMessage("Check connection failed because of an internal error.")
            .withInternalMessage("Check Pod failed to heartbeat, verify resource and heath of the worker/check pods.");
        break;
      case SCHEDULER_OVERLOADED:
        failureReason
            .withFailureOrigin(FailureOrigin.AIRBYTE_PLATFORM)
            .withExternalMessage("Airbyte Platform is experiencing a higher than usual load, please try again later.")
            .withInternalMessage("Check wasn't able to start within the expected time, check scheduler and worker load.");
        break;
      case OPERATION_TIMEOUT:
        failureReason
            .withExternalMessage("The check connection took too long.")
            .withInternalMessage(
                activity.getCheckConnectionTimeout() != null
                    ? String.format("Check connection exceeded the timeout of %s minutes", activity.getCheckConnectionTimeout().toMinutes())
                    : "The check connection took too long.");
        break;
      case UNKNOWN:
      case NOT_A_TIMEOUT:
      default:
        failureReason
            .withFailureOrigin(FailureOrigin.AIRBYTE_PLATFORM)
            .withExternalMessage("The check connection failed because of an internal error")
            .withInternalMessage("The check connection failed because of an internal error");
        break;
    }
    return failureReason;
  }

}
