/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.shared

import io.airbyte.config.ScheduleData
import java.util.UUID

data class ConnectionCronSchedule(
  val id: UUID,
  val scheduleData: ScheduleData,
)
