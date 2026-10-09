/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models.fusion

import java.util.UUID

data class ConnectionEnablementFlags(
  val enableIndexing: Boolean = false,
)

data class ConnectionEnablementState(
  val organizationId: UUID,
  val workspaceId: UUID,
  val connectionId: UUID,
  val sourceId: UUID,
  val destinationId: UUID,
  val flags: ConnectionEnablementFlags,
)
