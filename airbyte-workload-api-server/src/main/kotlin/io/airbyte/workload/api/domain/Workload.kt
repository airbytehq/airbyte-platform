/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api.domain

import io.airbyte.config.WorkloadPriority
import io.airbyte.config.WorkloadType
import io.swagger.v3.oas.annotations.media.Schema
import java.time.OffsetDateTime
import java.util.UUID

data class Workload(
  @Schema(required = true)
  var id: String = "",
  var dataplaneId: String? = null,
  var status: WorkloadStatus? = null,
  var labels: MutableList<WorkloadLabel> = mutableListOf(),
  var inputPayload: String = "",
  var workspaceId: UUID? = null,
  var organizationId: UUID? = null,
  var logPath: String = "",
  var mutexKey: String? = null,
  var type: WorkloadType = WorkloadType.SYNC,
  var terminationSource: String? = null,
  var terminationReason: String? = null,
  var deadline: OffsetDateTime? = null,
  // This is an uniq ID allowing to identify a workload. It is needed in addition of the workloadId to be able to add
  // this identifier to the kube pod label.
  var autoId: UUID = UUID.randomUUID(),
  var signalInput: String? = null,
  var dataplaneGroup: String? = null,
  var priority: WorkloadPriority? = null,
)
