/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models.dataworker

import io.airbyte.domain.models.DataplaneGroupId

data class DataplaneGroupDataWorkerUsage(
  val dataplaneGroupId: DataplaneGroupId,
  val dataplaneGroupName: String,
  val workspaces: List<WorkspaceDataWorkerUsage>,
)
