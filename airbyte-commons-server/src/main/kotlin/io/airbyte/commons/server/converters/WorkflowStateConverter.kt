/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters

import io.airbyte.api.model.generated.WorkflowStateRead
import io.airbyte.commons.temporal.scheduling.state.WorkflowState
import jakarta.inject.Singleton

/**
 * Convert between API and internal versions of workflow state models.
 */
@Singleton
class WorkflowStateConverter {
  fun getWorkflowStateRead(workflowState: WorkflowState): WorkflowStateRead = WorkflowStateRead().running(workflowState.isRunning)
}
