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

  override fun writeScopedConfiguration(scopedConfiguration: ScopedConfiguration): ScopedConfiguration {
    if (repository.existsById(scopedConfiguration.id)) {
      return repository.update(scopedConfiguration.toEntity()).toConfigModel()
    }

    return repository.save(scopedConfiguration.toEntity()).toConfigModel()
  }

  override fun listScopedConfigurations(): List<ScopedConfiguration> {
    return repository.findAll().map { it.toConfigModel() }.toList()
  }
}
