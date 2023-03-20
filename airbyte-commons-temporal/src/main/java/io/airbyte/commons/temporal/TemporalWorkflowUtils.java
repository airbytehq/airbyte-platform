/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.temporal.scheduling.ConnectionUpdaterInput;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.client.WorkflowOptions;
import io.temporal.common.RetryOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import java.time.Duration;
import java.util.UUID;

/**
 * Collection of Temporal workflow related utility methods.
 *
 * <b>N.B</b>: These methods should not store any state or depend on any other objects/singletons
 * managed by the application framework.
 */
public class TemporalWorkflowUtils {

  public static final RetryOptions NO_RETRY = RetryOptions.newBuilder().setMaximumAttempts(1).build();

  private TemporalWorkflowUtils() {}

  /**
   * Build starting input for the connection manager workflow.
   *
   * @param connectionId connection id
   * @return connection updated input
   */
  public static ConnectionUpdaterInput buildStartWorkflowInput(final UUID connectionId) {
    return ConnectionUpdaterInput.builder()
        .connectionId(connectionId)
        .jobId(null)
        .attemptId(null)
        .fromFailure(false)
        .attemptNumber(1)
        .workflowState(null)
        .resetConnection(false)
        .fromJobResetFailure(false)
        .build();
  }

  /**
   * Build workflow options from job type and workflow id.
   *
   * @param jobType job type
   * @param workflowId workflow id
   * @return workflow options
   */
  public static WorkflowOptions buildWorkflowOptions(final TemporalJobType jobType, final String workflowId) {
    return WorkflowOptions.newBuilder()
        .setWorkflowId(workflowId)
        .setRetryOptions(NO_RETRY)
        .setTaskQueue(jobType.name())
        .build();
  }

  /**
   * Build workflow options from job type.
   *
   * @param jobType job type
   * @return workflow options
   */
  public static WorkflowOptions buildWorkflowOptions(final TemporalJobType jobType) {
    return buildWorkflowOptionsWithTaskQueue(jobType.name());
  }

  /**
   * Build workflow options from task queue.
   *
   * @param taskQueue task queue
   * @return workflow options
   */
  public static WorkflowOptions buildWorkflowOptionsWithTaskQueue(final String taskQueue) {
    return WorkflowOptions.newBuilder()
        .setTaskQueue(taskQueue)
        .setWorkflowTaskTimeout(Duration.ofSeconds(27)) // TODO parker - temporarily increasing this to a recognizable number to see if it changes
        // error I'm seeing
        // todo (cgardens) we do not leverage Temporal retries.
        .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(1).build())
        .build();
  }

  /**
   * Create a run config for an attempt. For synchronous job the job id is a uuid.
   *
   * @param jobId job id
   * @param attemptId attempt id
   * @return information for a job run
   */
  public static JobRunConfig createJobRunConfig(final UUID jobId, final int attemptId) {
    return createJobRunConfig(String.valueOf(jobId), attemptId);
  }

  /**
   * Create a run config for an attempt. For async jobs the job id is a long
   *
   * @param jobId job id
   * @param attemptId attempt id
   * @return information for a job run
   */
  public static JobRunConfig createJobRunConfig(final long jobId, final int attemptId) {
    return createJobRunConfig(String.valueOf(jobId), attemptId);
  }

  /**
   * Create a run config for an attempt. Handles job ids as strings since synchronous ones can be
   * uuids and async ones can be longs.
   *
   * @param jobId job id
   * @param attemptId attempt id
   * @return information for a job run
   */
  public static JobRunConfig createJobRunConfig(final String jobId, final int attemptId) {
    return new JobRunConfig()
        .withJobId(jobId)
        .withAttemptId((long) attemptId);
  }

  /**
   * Get workflow service client.
   *
   * @param temporalHost temporal host
   * @return temporal service client
   */
  @VisibleForTesting
  public static WorkflowServiceStubsOptions getAirbyteTemporalOptions(final String temporalHost) {
    return WorkflowServiceStubsOptions.newBuilder()
        .setTarget(temporalHost)
        .build();
  }

  /**
   * Create temporal workflow client.
   *
   * @param workflowServiceStubs workflow service client
   * @param namespace temporal namespace
   * @return workflow client
   */
  public static WorkflowClient createWorkflowClient(final WorkflowServiceStubs workflowServiceStubs, final String namespace) {
    return WorkflowClient.newInstance(
        workflowServiceStubs,
        WorkflowClientOptions.newBuilder()
            .setNamespace(namespace)
            .build());
  }

}
