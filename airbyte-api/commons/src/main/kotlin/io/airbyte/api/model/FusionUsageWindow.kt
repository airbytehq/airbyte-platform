/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.model

import com.fasterxml.jackson.annotation.JsonInclude
import java.time.OffsetDateTime

@JsonInclude(JsonInclude.Include.ALWAYS)
data class FusionUsageWindow(
  val limit: Long?,
  val used: Long?,
  val windowStart: OffsetDateTime,
  val resetsAt: OffsetDateTime,
)
