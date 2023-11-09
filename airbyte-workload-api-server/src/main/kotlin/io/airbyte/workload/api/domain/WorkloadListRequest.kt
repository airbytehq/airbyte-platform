package io.airbyte.workload.api.domain

import java.time.OffsetDateTime

class WorkloadListRequest(
  var dataplane: List<String>? = null,
  var status: List<WorkloadStatus>? = null,
  var updatedBefore: OffsetDateTime? = null,
)
