/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models.dataworker

import io.airbyte.domain.models.WorkspaceId

data class WorkspaceDataWorkerUsage(
  val workspaceId: WorkspaceId,
  val workspaceName: String,
  val dataWorkers: List<DataWorkerUsageWithTime>,
)
