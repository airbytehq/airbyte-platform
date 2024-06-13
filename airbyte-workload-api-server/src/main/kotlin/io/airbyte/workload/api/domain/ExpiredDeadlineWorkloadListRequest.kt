package io.airbyte.workload.api.domain

import java.time.OffsetDateTime

data class ExpiredDeadlineWorkloadListRequest(
  var dataplane: List<String>? = null,
  var status: List<WorkloadStatus>? = null,
  var deadline: OffsetDateTime,
)
