/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.shared

import io.airbyte.config.ConfigOriginType
import io.airbyte.config.ConfigResourceType
import io.airbyte.config.ConfigScopeType
import java.util.Optional
import java.util.UUID

data class ScopedConfigurationMetadata(
  val id: UUID,
  val key: String,
  val scopeType: ConfigScopeType,
  val scopeId: UUID,
  val resourceType: Optional<ConfigResourceType>,
  val resourceId: Optional<UUID>,
  val originType: ConfigOriginType,
  val origin: String,
)
