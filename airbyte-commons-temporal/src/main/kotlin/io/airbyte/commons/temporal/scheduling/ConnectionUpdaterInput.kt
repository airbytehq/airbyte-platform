/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.commons.temporal.scheduling

import io.airbyte.commons.temporal.scheduling.state.WorkflowState
import java.util.UUID

/**
 * Input into a ConnectionManagerWorkflow. Used to pass information from one execution of the
 * ConnectionManagerWorkflow to the next.
 */
data class ConnectionUpdaterInput(
  var connectionId: UUID? = null,
  var jobId: Long? = null,
  var attemptId: Int? = null,
  var fromFailure: Boolean = false,
  var attemptNumber: Int? = null,
  /**
   * The state is needed because it has an event listener in it. The event listener only listen to
   * state updates which explains why it is a member of the [WorkflowState] class. The event
   * listener is currently (02/18/22) use for testing only.
   */
  var workflowState: WorkflowState? = null,
  var resetConnection: Boolean = false,
  var fromJobResetFailure: Boolean = false,
  var skipScheduling: Boolean = false,
) {
  constructor(connectionId: UUID, jobId: Long) : this(
    connectionId = connectionId,
    jobId = jobId,
    attemptId = null,
  )
}
