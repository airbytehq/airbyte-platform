package io.airbyte.data.services.impls.data

import io.airbyte.config.ConfigResourceType
import io.airbyte.config.ConfigSchema
import io.airbyte.config.ConfigScopeType
import io.airbyte.config.ScopedConfiguration
import io.airbyte.data.exceptions.ConfigNotFoundException
import io.airbyte.data.repositories.ScopedConfigurationRepository
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import io.airbyte.data.services.impls.data.mappers.toEntity
import io.airbyte.data.services.shared.ScopedConfigurationKey
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

@Singleton
class ScopedConfigurationServiceDataImpl(private val repository: ScopedConfigurationRepository) : ScopedConfigurationService {
  override fun getScopedConfiguration(configId: UUID): ScopedConfiguration {
    return repository.findById(configId).orElseThrow {
      ConfigNotFoundException(ConfigSchema.SCOPED_CONFIGURATION, configId)
    }.toConfigModel()
  }

  override fun getScopedConfiguration(
    key: String,
    resourceType: ConfigResourceType,
    resourceId: UUID,
    scopeType: ConfigScopeType,
    scopeId: UUID,
  ): Optional<ScopedConfiguration> {
    return Optional.ofNullable(
      repository.getByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeId(
        key,
        resourceType.toEntity(),
        resourceId,
        scopeType.toEntity(),
        scopeId,
      )?.toConfigModel(),
    )
  }

  override fun getScopedConfiguration(
    configKey: ScopedConfigurationKey,
    resourceType: ConfigResourceType,
    resourceId: UUID,
    scopeType: ConfigScopeType,
    scopeId: UUID,
  ): Optional<ScopedConfiguration> {
    if (!configKey.supportedScopes.contains(scopeType)) {
      throw IllegalArgumentException("Scope type $scopeType is not supported by key ${configKey.key}")
    }

    return getScopedConfiguration(
      configKey.key,
      resourceType,
      resourceId,
      scopeType,
      scopeId,
    )
  }

  override fun getScopedConfiguration(
    configKey: ScopedConfigurationKey,
    resourceType: ConfigResourceType,
    resourceId: UUID,
    scopes: Map<ConfigScopeType, UUID>,
  ): Optional<ScopedConfiguration> {
    scopes.keys.forEach {
      if (!configKey.supportedScopes.contains(it)) {
        throw IllegalArgumentException("Scope type $it is not supported by key ${configKey.key}")
      }
    }

    // Later down could optimize this to only make one query.
    for (scope in configKey.supportedScopes) {
      if (scopes.containsKey(scope)) {
        val scopedConfig =
          getScopedConfiguration(
            configKey.key,
            resourceType,
            resourceId,
            scope,
            scopes.getValue(scope),
          )
        if (scopedConfig.isPresent) {
          return scopedConfig
        }
      }
    }

    return Optional.empty()
  }

  override fun writeScopedConfiguration(scopedConfiguration: ScopedConfiguration): ScopedConfiguration {
    if (repository.existsById(scopedConfiguration.id)) {
      return repository.update(scopedConfiguration.toEntity()).toConfigModel()
    }

    return repository.save(scopedConfiguration.toEntity()).toConfigModel()
  }

  override fun listScopedConfigurations(): List<ScopedConfiguration> {
    return repository.findAll().map { it.toConfigModel() }.toList()
  }

  override fun deleteScopedConfiguration(configId: UUID) {
    repository.deleteById(configId)
  }
}
