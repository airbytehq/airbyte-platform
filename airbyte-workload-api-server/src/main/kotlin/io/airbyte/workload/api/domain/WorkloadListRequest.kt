package io.airbyte.workload.api.domain

import java.time.OffsetDateTime

data class WorkloadListRequest(
  var dataplane: List<String>? = null,
  var status: List<WorkloadStatus>? = null,
  var updatedBefore: OffsetDateTime? = null,
)
