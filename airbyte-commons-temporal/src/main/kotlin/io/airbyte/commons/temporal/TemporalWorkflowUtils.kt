/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal

import com.google.common.annotations.VisibleForTesting
import io.airbyte.commons.temporal.config.TemporalSdkTimeouts
import io.airbyte.commons.temporal.scheduling.ConnectionUpdaterInput
import io.airbyte.persistence.job.models.JobRunConfig
import io.temporal.client.WorkflowOptions
import io.temporal.common.RetryOptions
import io.temporal.serviceclient.WorkflowServiceStubsOptions
import java.time.Duration
import java.util.UUID

/**
 * Collection of Temporal workflow related utility methods.
 *
 * **N.B**: These methods should not store any state or depend on any other objects/singletons
 * managed by the application framework.
 */
object TemporalWorkflowUtils {
  val NO_RETRY: RetryOptions = RetryOptions.newBuilder().setMaximumAttempts(1).build()

  /**
   * Build starting input for the connection manager workflow.
   *
   * @param connectionId connection id
   * @return connection updated input
   */
  @JvmStatic
  fun buildStartWorkflowInput(connectionId: UUID?): ConnectionUpdaterInput =
    ConnectionUpdaterInput(
      connectionId,
      null,
      null,
      false,
      1,
      null,
      false,
      false,
      false,
    )

  /**
   * Build workflow options from job type and workflow id.
   *
   * @param jobType job type
   * @param workflowId workflow id
   * @return workflow options
   */
  fun buildWorkflowOptions(
    jobType: TemporalJobType,
    workflowId: String?,
  ): WorkflowOptions =
    WorkflowOptions
      .newBuilder()
      .setWorkflowId(workflowId)
      .setRetryOptions(NO_RETRY)
      .setTaskQueue(jobType.name)
      .build()

  /**
   * Build workflow options from job type.
   *
   * @param jobType job type
   * @return workflow options
   */
  fun buildWorkflowOptions(
    jobType: TemporalJobType,
    jobd: UUID,
  ): WorkflowOptions = buildWorkflowOptionsWithTaskQueue(jobType.name, jobd)

  /**
   * Build workflow options from task queue.
   *
   * @param taskQueue task queue
   * @return workflow options
   */
  fun buildWorkflowOptionsWithTaskQueue(
    taskQueue: String?,
    jobID: UUID,
  ): WorkflowOptions =
    WorkflowOptions
      .newBuilder()
      .setTaskQueue(taskQueue)
      .setWorkflowTaskTimeout(Duration.ofSeconds(27)) // TODO parker - temporarily increasing this to a recognizable number to see if it changes
      // error I'm seeing
      // todo (cgardens) we do not leverage Temporal retries.
      .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(1).build())
      .setWorkflowId(jobID.toString())
      .build()

  /**
   * Create a run config for an attempt. For synchronous job the job id is a uuid.
   *
   * @param jobId job id
   * @param attemptId attempt id
   * @return information for a job run
   */
  fun createJobRunConfig(
    jobId: UUID,
    attemptId: Int,
  ): JobRunConfig = createJobRunConfig(jobId.toString(), attemptId)

  /**
   * Create a run config for an attempt. For async jobs the job id is a long
   *
   * @param jobId job id
   * @param attemptId attempt id
   * @return information for a job run
   */
  @JvmStatic
  fun createJobRunConfig(
    jobId: Long,
    attemptId: Int,
  ): JobRunConfig = createJobRunConfig(jobId.toString(), attemptId)

  /**
   * Create a run config for an attempt. Handles job ids as strings since synchronous ones can be
   * uuids and async ones can be longs.
   *
   * @param jobId job id
   * @param attemptId attempt id
   * @return information for a job run
   */
  fun createJobRunConfig(
    jobId: String?,
    attemptId: Int,
  ): JobRunConfig =
    JobRunConfig()
      .withJobId(jobId)
      .withAttemptId(attemptId.toLong())

  /**
   * Get workflow service client.
   *
   * @param temporalHost temporal host
   * @param temporalSdkTimeouts The SDK RPC timeouts
   * @return temporal service client
   */
  @VisibleForTesting
  fun getAirbyteTemporalOptions(
    temporalHost: String?,
    temporalSdkTimeouts: TemporalSdkTimeouts,
  ): WorkflowServiceStubsOptions =
    WorkflowServiceStubsOptions
      .newBuilder()
      .setRpcTimeout(temporalSdkTimeouts.rpcTimeout)
      .setRpcLongPollTimeout(temporalSdkTimeouts.rpcLongPollTimeout)
      .setRpcQueryTimeout(temporalSdkTimeouts.rpcQueryTimeout)
      .setTarget(temporalHost)
      .build()
}
