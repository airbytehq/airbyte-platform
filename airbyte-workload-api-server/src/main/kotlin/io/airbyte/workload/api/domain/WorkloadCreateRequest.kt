package io.airbyte.workload.api.domain

import io.airbyte.config.WorkloadPriority
import io.airbyte.config.WorkloadType
import io.swagger.v3.oas.annotations.media.Schema
import java.time.OffsetDateTime

data class WorkloadCreateRequest(
  @Schema(required = true)
  var workloadId: String = "",
  var labels: List<WorkloadLabel> = listOf(),
  @Schema(required = true)
  var workloadInput: String = "",
  var logPath: String = "",
  var mutexKey: String? = null,
  var type: WorkloadType = WorkloadType.SYNC,
  var deadline: OffsetDateTime? = null,
  var priority: WorkloadPriority = WorkloadPriority.HIGH,
  var signalInput: String? = null,
)
