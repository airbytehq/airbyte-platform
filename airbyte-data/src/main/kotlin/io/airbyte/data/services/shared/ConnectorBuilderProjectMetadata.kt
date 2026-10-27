/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.shared

import java.util.Optional
import java.util.UUID

data class ConnectorBuilderProjectMetadata(
  val id: UUID,
  val workspace: Optional<UUID>,
  val definition: Optional<UUID> = Optional.empty(),
  val baseVersion: Optional<UUID> = Optional.empty(),
  val hasDraft: Boolean = false,
)
