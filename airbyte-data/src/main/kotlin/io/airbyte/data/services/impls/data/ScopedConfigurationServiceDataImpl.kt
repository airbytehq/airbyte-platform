package io.airbyte.data.services.impls.data

import io.airbyte.config.ConfigOriginType
import io.airbyte.config.ConfigResourceType
import io.airbyte.config.ConfigSchema
import io.airbyte.config.ConfigScopeType
import io.airbyte.config.ScopedConfiguration
import io.airbyte.data.exceptions.ConfigNotFoundException
import io.airbyte.data.repositories.ScopedConfigurationRepository
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import io.airbyte.data.services.impls.data.mappers.toEntity
import io.airbyte.data.services.shared.ConfigScopeMapWithId
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
    val id = UUID.randomUUID()
    val configsRes =
      getScopedConfigurations(
        configKey,
        resourceType,
        resourceId,
        listOf(ConfigScopeMapWithId(id, scopes)),
      )
    return Optional.ofNullable(configsRes[id])
  }

  override fun getScopedConfigurations(
    configKey: ScopedConfigurationKey,
    resourceType: ConfigResourceType,
    resourceId: UUID,
    scopeMaps: List<ConfigScopeMapWithId>,
  ): Map<UUID, ScopedConfiguration> {
    val idsPerScopeType =
      scopeMaps
        .flatMap { it.scopeMap.entries }
        .filter { it.value != null }
        .groupBy({ it.key }, { it.value!! })

    for (scopeType in idsPerScopeType.keys) {
      if (!configKey.supportedScopes.contains(scopeType)) {
        throw IllegalArgumentException("Scope type $scopeType is not supported by key ${configKey.key}")
      }
    }

    // Fetch all configs at once per type. This means max 1 query per scope type regardless of the number of entries in the input map.
    val configsPerScopeType =
      idsPerScopeType.mapValues { (scopeType, ids) ->
        listScopedConfigurationsWithScopes(configKey.key, resourceType, resourceId, scopeType, ids.toSet().toList())
      }

    val outMap = mutableMapOf<UUID, ScopedConfiguration>()
    for (scopeMapWithId in scopeMaps) {
      // Evaluate in priority order as defined by the config key.
      val scopeMap = scopeMapWithId.scopeMap
      for (scope in configKey.supportedScopes) {
        if (scopeMap.containsKey(scope)) {
          val scopeId = scopeMap.getValue(scope) ?: continue
          val scopedConfig = configsPerScopeType[scope]?.find { it.scopeId == scopeId }
          if (scopedConfig != null) {
            outMap[scopeMapWithId.id] = scopedConfig
            break
          }
        }
      }
    }

    return outMap
  }

  override fun writeScopedConfiguration(scopedConfiguration: ScopedConfiguration): ScopedConfiguration {
    if (repository.existsById(scopedConfiguration.id)) {
      return repository.update(scopedConfiguration.toEntity()).toConfigModel()
    }

    return repository.save(scopedConfiguration.toEntity()).toConfigModel()
  }

  override fun insertScopedConfigurations(scopedConfigurations: List<ScopedConfiguration>): List<ScopedConfiguration> {
    return repository.saveAll(scopedConfigurations.map { it.toEntity() }).map { it.toConfigModel() }
  }

  override fun listScopedConfigurations(): List<ScopedConfiguration> {
    return repository.findAll().map { it.toConfigModel() }.toList()
  }

  override fun listScopedConfigurations(key: String): List<ScopedConfiguration> {
    return repository.findByKey(key).map { it.toConfigModel() }.toList()
  }

  override fun listScopedConfigurationsWithScopes(
    key: String,
    resourceType: ConfigResourceType,
    resourceId: UUID,
    scopeType: ConfigScopeType,
    scopeIds: List<UUID>,
  ): List<ScopedConfiguration> {
    return repository.findByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeIdInList(
      key,
      resourceType.toEntity(),
      resourceId,
      scopeType.toEntity(),
      scopeIds,
    ).map { it.toConfigModel() }.toList()
  }

  override fun listScopedConfigurationsWithOrigins(
    key: String,
    resourceType: ConfigResourceType,
    resourceId: UUID,
    originType: ConfigOriginType,
    origins: List<String>,
  ): List<ScopedConfiguration> {
    return repository.findByKeyAndResourceTypeAndResourceIdAndOriginTypeAndOriginInList(
      key,
      resourceType.toEntity(),
      resourceId,
      originType.toEntity(),
      origins,
    ).map { it.toConfigModel() }.toList()
  }

  override fun deleteScopedConfiguration(configId: UUID) {
    repository.deleteById(configId)
  }

  override fun deleteScopedConfigurations(configIds: List<UUID>) {
    repository.deleteByIdInList(configIds)
  }
}
