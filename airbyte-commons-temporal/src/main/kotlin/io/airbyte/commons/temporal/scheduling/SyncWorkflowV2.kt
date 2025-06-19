/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.scheduling

import io.airbyte.config.StandardSyncOutput
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import java.util.UUID

data class SyncWorkflowV2Input(
  val connectionId: UUID,
  val jobId: Long,
  val attemptNumber: Long,
  val sourceId: UUID,
)

@WorkflowInterface
interface SyncWorkflowV2 {
  @WorkflowMethod
  fun run(input: SyncWorkflowV2Input): StandardSyncOutput
}
