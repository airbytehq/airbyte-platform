/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api.domain

import io.airbyte.config.WorkloadPriority

data class WorkloadQueueStats(
  var dataplaneGroup: String?,
  var priority: WorkloadPriority?,
  var enqueuedCount: Long,
)

data class WorkloadQueueStatsResponse(
  var stats: List<WorkloadQueueStats>,
)
