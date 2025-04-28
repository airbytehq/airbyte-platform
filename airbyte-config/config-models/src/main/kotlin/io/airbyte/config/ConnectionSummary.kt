/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import java.util.UUID

data class ConnectionSummary(
  val connectionId: UUID,
  val manual: Boolean?,
  val schedule: Schedule?,
  val sourceId: UUID,
  val destinationId: UUID,
)
