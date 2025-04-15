/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.ConfigOriginType
import io.airbyte.config.ConfigResourceType
import io.airbyte.config.ConfigScopeType
import io.airbyte.config.ScopedConfiguration
import io.airbyte.data.services.shared.ConfigScopeMapWithId
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

  fun getScopedConfiguration(
    key: String,
    scopeType: ConfigScopeType,
    scopeId: UUID,
  ): List<ScopedConfiguration>

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
   * Get a scoped configuration by key, resource and scope map.
   *
   * This will resolve the configuration by evaluating the scopes in the priority order defined by the given key.
   * Scopes included in the map must be defined as a supported scope in the key definition (see ScopedConfigurationKey).
   */
  fun getScopedConfigurations(
    configKey: ScopedConfigurationKey,
    scopes: Map<ConfigScopeType, UUID>,
    resourceType: ConfigResourceType,
  ): List<ScopedConfiguration>

  /**
   * Get a scoped configuration by key and scope map.
   *
   * This will resolve the configuration by evaluating the scopes in the priority order defined by the given key.
   * Scopes included in the map must be defined as a supported scope in the key definition (see ScopedConfigurationKey).
   */
  fun getScopedConfigurations(
    configKey: ScopedConfigurationKey,
    scopes: Map<ConfigScopeType, UUID>,
  ): List<ScopedConfiguration>

  /**
   * Get scoped configurations for multiple key, resource and scope map (in batch).
   *
   * This will resolve the configuration by evaluating the scopes in the priority order defined by the given key.
   * Scopes included in the map must be defined as a supported scope in the key definition (see ScopedConfigurationKey).
   *
   * IDs in the provided list of scope maps should be unique.
   * The same ID used in the input list will be used as the key in the output map, and the value will be the resolved configuration.
   * If no configuration exists for an ID, it will not be included in the output map.
   */
  fun getScopedConfigurations(
    configKey: ScopedConfigurationKey,
    resourceType: ConfigResourceType,
    resourceId: UUID,
    scopeMaps: List<ConfigScopeMapWithId>,
  ): Map<UUID, ScopedConfiguration>

  /**
   * Write a scoped configuration.
   */
  fun writeScopedConfiguration(scopedConfiguration: ScopedConfiguration): ScopedConfiguration

  /**
   * Insert multiple configurations.
   */
  fun insertScopedConfigurations(scopedConfigurations: List<ScopedConfiguration>): List<ScopedConfiguration>

  /**
   * List all scoped configurations.
   */
  fun listScopedConfigurations(): List<ScopedConfiguration>

  /**
   * List all scoped configurations with a certain key
   */
  fun listScopedConfigurations(key: String): List<ScopedConfiguration>

  /**
   * List scoped configurations with scope ids.
   */
  fun listScopedConfigurationsWithScopes(
    key: String,
    resourceType: ConfigResourceType,
    resourceId: UUID,
    scopeType: ConfigScopeType,
    scopeIds: List<UUID>,
  ): List<ScopedConfiguration>

  /**
   * List scoped configurations for an origin type.
   */
  fun listScopedConfigurations(originType: ConfigOriginType): List<ScopedConfiguration>

  /**
   * List scoped configurations with given origin values for an origin type.
   */
  fun listScopedConfigurationsWithOrigins(
    key: String,
    resourceType: ConfigResourceType,
    resourceId: UUID,
    originType: ConfigOriginType,
    origins: List<String>,
  ): List<ScopedConfiguration>

  /**
   * List scoped configurations with a given origin type and values.
   */
  fun listScopedConfigurationsWithValues(
    key: String,
    resourceType: ConfigResourceType,
    resourceId: UUID,
    scopeType: ConfigScopeType,
    originType: ConfigOriginType,
    values: List<String>,
  ): List<ScopedConfiguration>

  /**
   * Delete a scoped configuration by id.
   */
  fun deleteScopedConfiguration(configId: UUID)

  /**
   * Delete multiple configurations by their IDs.
   */
  fun deleteScopedConfigurations(configIds: List<UUID>)

  /**
   * Update the value for scoped configurations with given origin values for an origin type.
   */
  fun updateScopedConfigurationsOriginAndValuesForOriginInList(
    key: String,
    resourceType: ConfigResourceType,
    resourceId: UUID,
    originType: ConfigOriginType,
    origins: List<String>,
    newOrigin: String,
    newValue: String,
  )
}
