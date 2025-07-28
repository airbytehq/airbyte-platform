/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api.domain

data class WorkloadStatusUpdateRequest(
  var workloadId: String = "",
  var status: WorkloadStatus = WorkloadStatus.PENDING,
)
