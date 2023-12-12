/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal;

import io.temporal.activity.ActivityExecutionContext;
import io.temporal.client.ActivityCompletionException;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeartbeatUtils {

  private static final Logger log = LoggerFactory.getLogger(HeartbeatUtils.class);

  /**
   * Run a callable. If while it is running the temporal activity is cancelled, the provided callback
   * is triggered.
   * <p>
   * It manages this by regularly calling back to temporal in order to check whether the activity has
   * been cancelled. If it is cancelled it calls the callback.
   *
   * @param afterCancellationCallbackRef callback to be triggered if the temporal activity is
   *        cancelled before the callable completes
   * @param callable callable to run with cancellation
   * @param activityContext context used to check whether the activity has been cancelled
   * @param <T> type of variable returned by the callable
   * @return if the callable succeeds without being cancelled, returns the value returned by the
   *         callable
   */
  public static <T> T withBackgroundHeartbeat(final AtomicReference<Runnable> afterCancellationCallbackRef,
                                              final Callable<T> callable,
                                              final ActivityExecutionContext activityContext) {
    final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

    try {
      // Schedule the cancellation handler.
      scheduledExecutor.scheduleAtFixedRate(() -> {
        final CancellationHandler cancellationHandler = new CancellationHandler.TemporalCancellationHandler(activityContext);

        cancellationHandler.checkAndHandleCancellation(() -> {
          // After cancellation cleanup.
          if (afterCancellationCallbackRef != null) {
            final Runnable cancellationCallback = afterCancellationCallbackRef.get();
            if (cancellationCallback != null) {
              cancellationCallback.run();
            }
          }
        });
      }, 0, TemporalConstants.SEND_HEARTBEAT_INTERVAL.toSeconds(), TimeUnit.SECONDS);

      return callable.call();
    } catch (final ActivityCompletionException e) {
      log.warn("Job either timed out or was cancelled.");
      throw new RuntimeException(e);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    } finally {
      log.info("Stopping temporal heartbeating...");
      // At this point we are done, we should try to stop all heartbeat ASAP.
      // We can safely ignore the list of tasks that didn't start since we are exiting regardless.
      scheduledExecutor.shutdownNow();

      try {
        // Making sure the heartbeat executor is terminated to avoid heartbeat attempt after we're done with
        // the activity.
        if (scheduledExecutor.awaitTermination(TemporalConstants.HEARTBEAT_SHUTDOWN_GRACE_PERIOD.toSeconds(), TimeUnit.SECONDS)) {
          log.info("Temporal heartbeating stopped.");
        } else {
          // Heartbeat thread failed to stop, we may leak a thread if this happens.
          // We should not fail the execution because of this.
          log.info(
              "Temporal heartbeating didn't stop within {} seconds, continuing the shutdown. (WorkflowId={}, ActivityId={}, RunId={})",
              TemporalConstants.HEARTBEAT_SHUTDOWN_GRACE_PERIOD.toSeconds(), activityContext.getInfo().getWorkflowId(),
              activityContext.getInfo().getActivityId(), activityContext.getInfo().getRunId());
        }
      } catch (InterruptedException e) {
        // We got interrupted while attempting to shutdown the executor. Not much more we can do.
        log.info("Interrupted while stopping the Temporal heartbeating, continuing the shutdown.");
        // Preserve the interrupt status
        Thread.currentThread().interrupt();
      }
    }
  }

}
