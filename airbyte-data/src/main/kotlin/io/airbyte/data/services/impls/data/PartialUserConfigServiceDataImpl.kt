/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.config.PartialUserConfig
import io.airbyte.config.PartialUserConfigWithActorDetails
import io.airbyte.data.repositories.PartialUserConfigRepository
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
) : PartialUserConfigService {
  override fun getPartialUserConfig(partialUserConfigId: UUID): PartialUserConfigWithActorDetails {
    val partialUserConfig =
      repository
        .findById(partialUserConfigId)
        .orElseThrow {
          throw RuntimeException("PartialUserConfig not found")
        }.toConfigModel()
    val sourceDefinition = sourceService.getSourceDefinitionFromSource(partialUserConfig.sourceId)

    return PartialUserConfigWithActorDetails(
      partialUserConfig = partialUserConfig,
      actorName = sourceDefinition.name,
      actorIcon = sourceDefinition.iconUrl,
    )
  }

  override fun listPartialUserConfigs(workspaceId: UUID): List<PartialUserConfigWithActorDetails> {
    val partialUserConfigs = repository.findByWorkspaceId(workspaceId).map { it.toConfigModel() }
    val sourceDefinitions = partialUserConfigs.map { partialUserConfig -> sourceService.getSourceDefinitionFromSource(partialUserConfig.sourceId) }

    return partialUserConfigs.mapIndexed { index, partialUserConfig ->
      PartialUserConfigWithActorDetails(
        partialUserConfig = partialUserConfig,
        actorName = sourceDefinitions[index].name,
        actorIcon = sourceDefinitions[index].iconUrl,
      )
    }
  }

  override fun createPartialUserConfig(partialUserConfigCreate: PartialUserConfig): PartialUserConfigWithActorDetails {
    val storedPartialUserConfig = repository.save(partialUserConfigCreate.toEntity()).toConfigModel()
    val sourceDefinition = sourceService.getSourceDefinitionFromSource(storedPartialUserConfig.sourceId)
    return PartialUserConfigWithActorDetails(
      partialUserConfig = storedPartialUserConfig,
      actorName = sourceDefinition.name,
      actorIcon = sourceDefinition.iconUrl,
    )
  }

  override fun updatePartialUserConfig(partialUserConfig: PartialUserConfig): PartialUserConfigWithActorDetails {
    // Check if the config exists before updating
    repository
      .findById(partialUserConfig.id)
      .orElseThrow {
        throw RuntimeException("PartialUserConfig not found for update")
      }

    val updatedPartialUserConfig = repository.save(partialUserConfig.toEntity()).toConfigModel()
    val sourceDefinition = sourceService.getSourceDefinitionFromSource(updatedPartialUserConfig.sourceId)

    return PartialUserConfigWithActorDetails(
      partialUserConfig = updatedPartialUserConfig,
      actorName = sourceDefinition.name,
      actorIcon = sourceDefinition.iconUrl,
    )
  }
}
