/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence.versionoverrides

import io.airbyte.config.ConfigOriginType
import io.airbyte.config.ConfigResourceType
import io.airbyte.config.ConfigScopeType
import io.airbyte.config.ScopedConfiguration
import io.airbyte.config.persistence.ActorDefinitionVersionHelper.ActorDefinitionVersionWithOverrideStatus
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.shared.ConnectorVersionKey
import io.airbyte.validation.json.JsonValidationException
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.io.IOException
import java.util.EnumMap
import java.util.Optional
import java.util.UUID
import java.util.function.Function
import java.util.stream.Collectors

@Singleton
@Named("configurationVersionOverrideProvider")
class ConfigurationDefinitionVersionOverrideProvider(
  private val workspaceService: WorkspaceService,
  private val actorDefinitionService: ActorDefinitionService,
  private val scopedConfigurationService: ScopedConfigurationService,
) : DefinitionVersionOverrideProvider {
  private fun getOrganizationId(workspaceId: UUID): UUID {
    try {
      val workspace = workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true)
      return workspace.organizationId
    } catch (e: ConfigNotFoundException) {
      throw RuntimeException(e)
    } catch (e: IOException) {
      throw RuntimeException(e)
    } catch (e: JsonValidationException) {
      throw RuntimeException(e)
    }
  }

  private fun getScopedConfig(
    actorDefinitionId: UUID,
    workspaceId: UUID,
    actorId: UUID?,
  ): Optional<ScopedConfiguration> {
    val organizationId = getOrganizationId(workspaceId)

    val scopes: MutableMap<ConfigScopeType, UUID> = EnumMap(java.util.Map.of(ConfigScopeType.WORKSPACE, workspaceId))

    // TODO: This should always be true now. We should probably warn in a log line if this is not null.
    if (organizationId != null) {
      scopes[ConfigScopeType.ORGANIZATION] = organizationId
    }

    if (actorId != null) {
      scopes[ConfigScopeType.ACTOR] = actorId
    }

    return scopedConfigurationService.getScopedConfiguration(
      ConnectorVersionKey,
      ConfigResourceType.ACTOR_DEFINITION,
      actorDefinitionId,
      scopes,
    )
  }

  override fun getOverride(
    actorDefinitionId: UUID,
    workspaceId: UUID,
    actorId: UUID?,
  ): Optional<ActorDefinitionVersionWithOverrideStatus> {
    val optConfig = getScopedConfig(actorDefinitionId, workspaceId, actorId)
    if (optConfig.isPresent) {
      val config = optConfig.get()
      try {
        val version = actorDefinitionService.getActorDefinitionVersion(UUID.fromString(config.value))
        val isManualOverride = config.originType == ConfigOriginType.USER
        return Optional.of(ActorDefinitionVersionWithOverrideStatus(version, isManualOverride))
      } catch (e: ConfigNotFoundException) {
        throw RuntimeException(e)
      } catch (e: IOException) {
        throw RuntimeException(e)
      }
    }
    return Optional.empty()
  }

  override fun getOverrides(
    actorDefinitionIds: List<UUID>,
    workspaceId: UUID,
  ): List<ActorDefinitionVersionWithOverrideStatus> {
    val scopes: Map<ConfigScopeType, UUID> =
      EnumMap(
        java.util.Map.of(
          ConfigScopeType.WORKSPACE,
          workspaceId,
          ConfigScopeType.ORGANIZATION,
          getOrganizationId(workspaceId),
        ),
      )

    // Convert the List into a map for fast lookups below
    val actorDefinitionMap =
      actorDefinitionIds
        .stream()
        .collect(
          Collectors
            .toMap(
              Function.identity(),
              Function.identity(),
            ),
        )

    val scopedConfigs =
      scopedConfigurationService
        .getScopedConfigurations(
          ConnectorVersionKey,
          scopes,
          ConfigResourceType.ACTOR_DEFINITION,
        ).stream()
        .filter { scopedConfiguration: ScopedConfiguration -> actorDefinitionMap.containsKey(scopedConfiguration.resourceId) }
        .collect(
          Collectors
            .toMap(
              { obj: ScopedConfiguration -> obj.resourceId },
              Function.identity(),
            ),
        )

    val overrides: MutableList<ActorDefinitionVersionWithOverrideStatus> = ArrayList()
    try {
      for (actorDefinitionVersion in actorDefinitionService
        .getActorDefinitionVersions(
          scopedConfigs.values
            .stream()
            .map { config: ScopedConfiguration -> UUID.fromString(config.value) }
            .toList(),
        )) {
        val isManualOverride = scopedConfigs[actorDefinitionVersion.actorDefinitionId]!!.originType == ConfigOriginType.USER
        overrides.add(ActorDefinitionVersionWithOverrideStatus(actorDefinitionVersion, isManualOverride))
      }
    } catch (e: IOException) {
      throw RuntimeException(e)
    }
    return overrides
  }
}
