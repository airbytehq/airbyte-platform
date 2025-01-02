/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.commons.temporal.scheduling

import io.airbyte.commons.temporal.scheduling.state.WorkflowState
import io.temporal.workflow.QueryMethod
import io.temporal.workflow.SignalMethod
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod

/**
 * Temporal workflow that manages running sync jobs for a connection. It handles scheduling, the
 * whole job / attempt lifecycle, and executing the sync.
 *
 * // todo (cgardens) - ideally we could rebuild this to just manage scheduling and job lifecycle and
 * // not know anything about syncs. Right now multiple concepts are smashed into this one house.
 */
@WorkflowInterface
interface ConnectionManagerWorkflow {
  /**
   * Workflow method to launch a [ConnectionManagerWorkflow]. Launches a workflow responsible
   * for scheduling syncs. This workflow will run and then continue running until deleted.
   */
  @WorkflowMethod
  fun run(connectionUpdaterInput: ConnectionUpdaterInput?)

  /**
   * Send a signal that will bypass the waiting time and run a sync. Nothing will happen if a sync is
   * already running.
   */
  @SignalMethod
  fun submitManualSync()

  /**
   * Cancel all the current executions of a sync and mark the set the status of the job as canceled.
   * Nothing will happen if a sync is not running.
   */
  @SignalMethod
  fun cancelJob()

  /**
   * Cancel a running workflow and then delete the connection and finally make the workflow to stop
   * instead of continuing as new.
   */
  @SignalMethod
  fun deleteConnection()

  /**
   * Signal that the connection config has been updated. If nothing was currently running, it will
   * continue the workflow as new, which will reload the config. Nothing will happend if a sync is
   * running.
   */
  @SignalMethod
  fun connectionUpdated()

  @SignalMethod
  fun resetConnection()

  @SignalMethod
  fun resetConnectionAndSkipNextScheduling()

  @QueryMethod
  fun getState(): WorkflowState

  @QueryMethod
  fun getJobInformation(): JobInformation

  /**
   * Job Attempt Information.
   */
  data class JobInformation(
    var jobId: Long = 0,
    var attemptId: Int = 0,
  )

  companion object {
    @JvmField
    val NON_RUNNING_JOB_ID: Long = -1

    @JvmField
    val NON_RUNNING_ATTEMPT_ID: Int = -1
  }
}
