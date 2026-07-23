/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api.domain

import io.swagger.v3.oas.annotations.media.Schema
import java.time.OffsetDateTime

data class WorkloadClaimRequest(
  @Schema(required = true)
  var workloadId: String = "",
  @Schema(required = true)
  var dataplaneId: String = "",
  var deadline: OffsetDateTime? = null,
)
