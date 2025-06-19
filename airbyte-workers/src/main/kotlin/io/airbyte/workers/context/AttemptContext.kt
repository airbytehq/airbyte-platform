/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.context

import io.airbyte.metrics.lib.ApmTraceConstants
import io.airbyte.metrics.lib.ApmTraceUtils
import java.util.UUID

data class AttemptContext(
  val connectionId: UUID?,
  val jobId: Long?,
  val attemptNumber: Int?,
) {
  /**
   * Update the current trace with the ids from the context.
   */
  fun addTagsToTrace() {
    val tags: MutableMap<String?, Any?> = mutableMapOf()
    if (connectionId != null) {
      tags.put(ApmTraceConstants.Tags.CONNECTION_ID_KEY, connectionId)
    }
    if (jobId != null) {
      tags.put(ApmTraceConstants.Tags.JOB_ID_KEY, jobId)
    }
    if (attemptNumber != null) {
      tags.put(ApmTraceConstants.Tags.ATTEMPT_NUMBER_KEY, attemptNumber)
    }
    ApmTraceUtils.addTagsToTrace(tags)
  }
}
