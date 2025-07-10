/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.config.PartialUserConfig
import io.airbyte.config.PartialUserConfigWithActorDetails
import io.airbyte.config.PartialUserConfigWithConfigTemplateAndActorDetails
import io.airbyte.data.repositories.PartialUserConfigRepository
import io.airbyte.data.services.ConfigTemplateService
import io.airbyte.data.services.PartialUserConfigService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import io.airbyte.data.services.impls.data.mappers.toEntity
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
open class PartialUserConfigServiceDataImpl(
  private val repository: PartialUserConfigRepository,
  private val sourceService: SourceService,
  private val configTemplateService: ConfigTemplateService,
) : PartialUserConfigService {
  override fun getPartialUserConfig(partialUserConfigId: UUID): PartialUserConfigWithConfigTemplateAndActorDetails {
    val partialUserConfig =
      repository
        .findByIdAndTombstoneFalse(partialUserConfigId)
        .orElseThrow {
          throw RuntimeException("PartialUserConfig not found")
        }.toConfigModel()

    val configTemplate = configTemplateService.getConfigTemplate(partialUserConfig.configTemplateId, partialUserConfig.workspaceId)

    return PartialUserConfigWithConfigTemplateAndActorDetails(
      partialUserConfig = partialUserConfig,
      actorName = configTemplate.actorName,
      actorIcon = configTemplate.actorIcon,
      configTemplate = configTemplate.configTemplate,
    )
  }

  override fun listPartialUserConfigs(workspaceId: UUID): List<PartialUserConfigWithActorDetails> {
    val partialUserConfigs = repository.findByWorkspaceIdAndTombstoneFalse(workspaceId).map { it.toConfigModel() }
    val sourceDefinitions = partialUserConfigs.map { partialUserConfig -> sourceService.getSourceDefinitionFromSource(partialUserConfig.actorId!!) }

    return partialUserConfigs.mapIndexed { index, partialUserConfig ->
      PartialUserConfigWithActorDetails(
        partialUserConfig = partialUserConfig,
        actorName = sourceDefinitions[index].name,
        actorIcon = sourceDefinitions[index].iconUrl,
        configTemplateId = partialUserConfig.configTemplateId,
      )
    }
  }

  override fun createPartialUserConfig(partialUserConfigCreate: PartialUserConfig): PartialUserConfigWithActorDetails {
    val storedPartialUserConfig = repository.save(partialUserConfigCreate.toEntity()).toConfigModel()
    val sourceDefinition = sourceService.getSourceDefinitionFromSource(storedPartialUserConfig.actorId!!)
    return PartialUserConfigWithActorDetails(
      partialUserConfig = storedPartialUserConfig,
      actorName = sourceDefinition.name,
      actorIcon = sourceDefinition.iconUrl,
      configTemplateId = storedPartialUserConfig.configTemplateId,
    )
  }

  override fun updatePartialUserConfig(partialUserConfig: PartialUserConfig): PartialUserConfigWithActorDetails {
    // Check if the config exists before updating
    repository
      .findById(partialUserConfig.id)
      .orElseThrow {
        throw RuntimeException("PartialUserConfig not found for update")
      }

    val updatedPartialUserConfig = repository.update(partialUserConfig.toEntity()).toConfigModel()
    val sourceDefinition = sourceService.getSourceDefinitionFromSource(updatedPartialUserConfig.actorId!!)

    return PartialUserConfigWithActorDetails(
      partialUserConfig = updatedPartialUserConfig,
      actorName = sourceDefinition.name,
      actorIcon = sourceDefinition.iconUrl,
      configTemplateId = updatedPartialUserConfig.configTemplateId,
    )
  }

  override fun deletePartialUserConfig(partialUserConfigId: UUID) {
    val partialUserConfig =
      repository
        .findById(partialUserConfigId)
        .orElseThrow {
          throw RuntimeException("PartialUserConfig not found for delete")
        }

    partialUserConfig.tombstone = true

    repository.update(partialUserConfig)
  }

  override fun deletePartialUserConfigForSource(sourceId: UUID) {
    val partialUserConfigs = repository.findByActorId(sourceId)
    if (partialUserConfigs.isEmpty()) {
      return
    }

    partialUserConfigs.forEach { it.id?.let { foundConfigId -> deletePartialUserConfig(foundConfigId) } }
  }
}
