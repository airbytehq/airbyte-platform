/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.scheduling

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import io.airbyte.config.StandardSyncOutput
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import java.util.UUID

@JsonDeserialize(builder = SyncWorkflowV2Input.Builder::class)
data class SyncWorkflowV2Input(
  val connectionId: UUID,
  val jobId: Long,
  val attemptNumber: Long,
  val sourceId: UUID,
) {
  // Temporal tests requires us to have a builder. We don't run into this in the prod workflow.
  class Builder
    @JvmOverloads
    constructor(
      var connectionId: UUID? = null,
      var jobId: Long? = null,
      var attemptNumber: Long? = null,
      var sourceId: UUID? = null,
    ) {
      fun connectionId(value: UUID) = apply { this.connectionId = value }

      fun jobId(value: Long) = apply { this.jobId = value }

      fun attemptNumber(value: Long) = apply { this.attemptNumber = value }

      fun sourceId(value: UUID) = apply { this.sourceId = value }

      fun build() =
        SyncWorkflowV2Input(
          connectionId = connectionId ?: error("connectionId must be set"),
          jobId = jobId ?: error("jobId must be set"),
          attemptNumber = attemptNumber ?: error("attemptNumber must be set"),
          sourceId = sourceId ?: error("sourceId must be set"),
        )
    }
}

@WorkflowInterface
interface SyncWorkflowV2 {
  @WorkflowMethod
  fun run(input: SyncWorkflowV2Input): StandardSyncOutput
}
