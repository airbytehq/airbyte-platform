/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api.domain

import io.swagger.v3.oas.annotations.media.Schema
import java.time.OffsetDateTime

class WorkloadLaunchedRequest(
  @Schema(required = true)
  var workloadId: String = "",
  var deadline: OffsetDateTime? = null,
)
