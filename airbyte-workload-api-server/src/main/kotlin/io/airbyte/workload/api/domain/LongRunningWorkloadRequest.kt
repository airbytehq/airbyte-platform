package io.airbyte.workload.api.domain

import java.time.OffsetDateTime

data class LongRunningWorkloadRequest(
  var dataplane: List<String>? = null,
  var createdBefore: OffsetDateTime? = null,
)
