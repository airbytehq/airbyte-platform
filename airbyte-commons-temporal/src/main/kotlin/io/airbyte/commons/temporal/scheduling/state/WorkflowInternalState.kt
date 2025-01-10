/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.scheduling.state

import io.airbyte.config.FailureReason

/**
 * Internal state of workflow.
 * // todo (cgardens) - how is this different from WorkflowState.
 */
data class WorkflowInternalState(
  var jobId: Long? = null,
  /** 0-based incrementing sequence. */
  var attemptNumber: Int? = null,
  var failures: MutableSet<FailureReason> = mutableSetOf(),
  var partialSuccess: Boolean? = null,
) {
  constructor() : this(jobId = null, attemptNumber = null, failures = mutableSetOf(), partialSuccess = null)
}
