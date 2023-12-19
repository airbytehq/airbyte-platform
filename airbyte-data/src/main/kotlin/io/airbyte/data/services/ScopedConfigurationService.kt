package io.airbyte.data.services

import io.airbyte.config.ConfigResourceType
import io.airbyte.config.ConfigScopeType
import io.airbyte.config.ScopedConfiguration
import java.util.Optional
import java.util.UUID

/**
 * A service that manages scoped configurations.
 */
interface ScopedConfigurationService {
  fun getScopedConfiguration(configId: UUID): ScopedConfiguration

  fun getScopedConfiguration(
    key: String,
    resourceType: ConfigResourceType,
    resourceId: UUID,
    scopeType: ConfigScopeType,
    scopeId: UUID,
  ): Optional<ScopedConfiguration>

  fun writeScopedConfiguration(scopedConfiguration: ScopedConfiguration): ScopedConfiguration

  fun listScopedConfigurations(): List<ScopedConfiguration>
}
