package io.airbyte.data.services

import io.airbyte.config.ConfigResourceType
import io.airbyte.config.ConfigScopeType
import io.airbyte.config.ScopedConfiguration
import io.airbyte.data.services.shared.ScopedConfigurationKey
import java.util.Optional
import java.util.UUID

/**
 * A service that manages scoped configurations.
 */
interface ScopedConfigurationService {
  /**
   * Get a scoped configuration by its id.
   */
  fun getScopedConfiguration(configId: UUID): ScopedConfiguration

  /**
   * Get a scoped configuration by key string, resource and scope.
   */
  fun getScopedConfiguration(
    key: String,
    resourceType: ConfigResourceType,
    resourceId: UUID,
    scopeType: ConfigScopeType,
    scopeId: UUID,
  ): Optional<ScopedConfiguration>

  /**
   * Get a scoped configuration by key, resource and scope.
   */
  fun getScopedConfiguration(
    configKey: ScopedConfigurationKey,
    resourceType: ConfigResourceType,
    resourceId: UUID,
    scopeType: ConfigScopeType,
    scopeId: UUID,
  ): Optional<ScopedConfiguration>

  /**
   * Get a scoped configuration by key, resource and scope map.
   *
   * This will resolve the configuration by evaluating the scopes in the priority order defined by the given key.
   * Scopes included in the map must be defined as a supported scope in the key definition (see ScopedConfigurationKey).
   */
  fun getScopedConfiguration(
    configKey: ScopedConfigurationKey,
    resourceType: ConfigResourceType,
    resourceId: UUID,
    scopes: Map<ConfigScopeType, UUID>,
  ): Optional<ScopedConfiguration>

  /**
   * Write a scoped configuration.
   */
  fun writeScopedConfiguration(scopedConfiguration: ScopedConfiguration): ScopedConfiguration

  /**
   * List all scoped configurations.
   */
  fun listScopedConfigurations(): List<ScopedConfiguration>
}
