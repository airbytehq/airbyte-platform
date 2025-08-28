/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api.domain

import io.swagger.v3.oas.annotations.media.Schema
import java.time.OffsetDateTime

data class WorkloadSummary(
  @Schema(required = true)
  var id: String = "",
  @Schema(required = true)
  var autoId: String = "",
  var status: WorkloadStatus? = null,
  var deadline: OffsetDateTime? = null,
)
