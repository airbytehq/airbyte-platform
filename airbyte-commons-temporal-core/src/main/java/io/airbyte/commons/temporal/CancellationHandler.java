/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal;

import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.FAILURE_TYPES_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.TEMPORAL_ACTIVITY_ID_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.TEMPORAL_WORKFLOW_ID_KEY;

import io.airbyte.commons.temporal.exception.RetryableException;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.temporal.activity.ActivityExecutionContext;
import io.temporal.client.ActivityCanceledException;
import io.temporal.client.ActivityCompletionException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For each call, checks if the current activity is cancelled. If it is, then it executes the
 * provided callback.
 */
public interface CancellationHandler {

  void checkAndHandleCancellation(Runnable onCancellationCallback);

  /**
   * Temporal implementation of the cancellation handler.
   */
  class TemporalCancellationHandler implements CancellationHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(TemporalCancellationHandler.class);

    private final ActivityExecutionContext activityContext;

    public TemporalCancellationHandler(final ActivityExecutionContext activityContext) {
      this.activityContext = activityContext;
    }

    /**
     * Check for a cancellation/timeout status and run any callbacks necessary to shut down underlying
     * processes. This method should generally be run frequently within an activity so a change in
     * cancellation status is respected. This will only be effective if the cancellation type for the
     * workflow is set to
     * {@link io.temporal.activity.ActivityCancellationType#WAIT_CANCELLATION_COMPLETED}; otherwise, the
     * activity will be killed automatically as part of cleanup without removing underlying processes.
     *
     * @param onCancellationCallback a runnable that will only run when Temporal indicates the activity
     *        should be killed (cancellation or timeout).
     */
    @Override
    public void checkAndHandleCancellation(final Runnable onCancellationCallback) {
      try {
        ApmTraceUtils.addTagsToTrace(Map.of(
            TEMPORAL_ACTIVITY_ID_KEY, activityContext.getInfo().getActivityId(),
            TEMPORAL_WORKFLOW_ID_KEY, activityContext.getInfo().getWorkflowId()));
        /*
         * Heartbeat is somewhat misleading here. What it does is check the current Temporal activity's
         * context and throw an exception if the sync has been cancelled or timed out. The input to this
         * heartbeat function is available as a field in thrown ActivityCompletionExceptions, which we
         * aren't using for now.
         *
         * We should use this only as a check for the ActivityCompletionException. See {@link
         * TemporalUtils#withBackgroundHeartbeat} for where we actually send heartbeats to ensure that we
         * don't time out the activity.
         */
        activityContext.heartbeat(null);
      } catch (final ActivityCanceledException e) {
        ApmTraceUtils.addExceptionToTrace(e);
        ApmTraceUtils.addTagsToTrace(Map.of(FAILURE_TYPES_KEY, e.getClass().getName()));
        onCancellationCallback.run();
        LOGGER.warn("Job was cancelled.", e);
      } catch (final ActivityCompletionException e) {
        ApmTraceUtils.addExceptionToTrace(e);
        ApmTraceUtils.addTagsToTrace(Map.of(FAILURE_TYPES_KEY, e.getClass().getName()));
        // TODO: This is a hack to avoid having to manually destroy pod, it should be revisited
        if (!e.getWorkflowId().orElse("").toLowerCase().startsWith("sync")) {
          LOGGER.warn("The job timeout and was not a sync, we will destroy the pods related to it", e);
          onCancellationCallback.run();
        } else {
          LOGGER.debug(
              "An error happened while checking that the temporal activity is still alive but is not a cancellation, forcing the activity to retry",
              e);
        }
        throw new RetryableException(e);
      }
    }

  }

}
