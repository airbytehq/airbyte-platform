package io.airbyte.workload.api.domain

data class WorkloadStatusUpdateRequest(
  var workloadId: String = "",
  var status: WorkloadStatus = WorkloadStatus.PENDING,
)
