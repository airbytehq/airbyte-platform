package io.airbyte.data.services.shared

import io.airbyte.config.ConfigScopeType
import java.util.UUID

/**
 * Data class to associate an ID with a given ScopedConfiguration scope map.
 * This is used for resolving scopes in bulk, see ScopedConfigurationService.
 */
data class ConfigScopeMapWithId(
  val id: UUID,
  val scopeMap: Map<ConfigScopeType, UUID?>,
)
