/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models.dataworker

import java.time.OffsetDateTime

data class DataWorkerUsageWithTime(
  val date: OffsetDateTime,
  val usage: Double,
)
