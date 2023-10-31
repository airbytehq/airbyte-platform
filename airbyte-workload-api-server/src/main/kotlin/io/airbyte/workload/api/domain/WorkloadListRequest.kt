package io.airbyte.workload.api.domain

import java.time.OffsetDateTime

class WorkloadListRequest(
  var dataplane: MutableList<String>? = null,
  var status: MutableList<WorkloadStatus>? = null,
  var updatedBefore: OffsetDateTime? = null,
)
