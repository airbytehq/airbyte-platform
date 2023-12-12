package io.airbyte.workload.api.domain

data class WorkloadListResponse(
  var workloads: List<Workload> = ArrayList(),
)
