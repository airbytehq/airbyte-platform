package io.airbyte.workload.api.domain

data class WorkloadListResponse(
  var workloads: MutableList<Workload> = ArrayList(),
)
