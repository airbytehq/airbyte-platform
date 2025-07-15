/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import io.airbyte.commons.version.Version
import io.airbyte.config.ActorDefinitionBreakingChange
import io.airbyte.config.ActorType
import io.airbyte.config.ConfigOriginType
import io.airbyte.config.ConfigResourceType
import io.airbyte.config.ConfigScopeType
import io.airbyte.config.ScopedConfiguration
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.shared.ConnectorVersionKey
import io.airbyte.data.services.shared.StandardSyncQuery
import jakarta.inject.Singleton
import java.io.IOException
import java.util.UUID
import java.util.function.Function
import java.util.stream.Collectors

/**
 * Helper class containing logic related to breaking changes.
 */
@Singleton
class BreakingChangesHelper(
  private val scopedConfigurationService: ScopedConfigurationService,
  private val workspaceService: WorkspaceService,
  private val destinationService: DestinationService,
  private val sourceService: SourceService,
) {
  @JvmRecord
  data class WorkspaceBreakingChangeInfo(
    val workspaceId: UUID,
    val connectionId: List<UUID>,
    val scopedConfigurations: List<ScopedConfiguration>,
  )

  /**
   * Finds all breaking change pins on versions that are unsupported due to a breaking change. Results
   * are given per workspace.
   *
   * @param actorType - type of actor
   * @param actorDefinitionId - actor definition id
   * @param unsupportedVersionIds - unsupported version ids
   * @return list of workspace ids with active syncs on unsupported versions, along with the sync ids
   * and the scopedConfigurations entries
   */
  @Throws(IOException::class)
  fun getBreakingActiveSyncsPerWorkspace(
    actorType: ActorType,
    actorDefinitionId: UUID,
    unsupportedVersionIds: List<UUID>,
  ): List<WorkspaceBreakingChangeInfo> {
    // get actors pinned to unsupported versions (due to a breaking change)
    val pinnedValues = unsupportedVersionIds.stream().map { obj: UUID -> obj.toString() }.toList()
    val breakingChangePins =
      scopedConfigurationService.listScopedConfigurationsWithValues(
        ConnectorVersionKey.key,
        ConfigResourceType.ACTOR_DEFINITION,
        actorDefinitionId,
        ConfigScopeType.ACTOR,
        ConfigOriginType.BREAKING_CHANGE,
        pinnedValues,
      )

    // fetch actors and group by workspace
    val actorIdsByWorkspace = getActorIdsByWorkspace(actorType, breakingChangePins)
    val scopedConfigurationByActorId =
      breakingChangePins.stream().collect(
        Collectors.toMap(
          Function { obj: ScopedConfiguration -> obj.scopeId },
          Function.identity(),
        ),
      )

    // get affected syncs for each workspace
    val returnValue: MutableList<WorkspaceBreakingChangeInfo> = ArrayList()
    for ((workspaceId, actorIdsForWorkspace) in actorIdsByWorkspace) {
      val syncQuery = buildStandardSyncQuery(actorType, workspaceId, actorIdsForWorkspace)
      val activeSyncIds = workspaceService.listWorkspaceActiveSyncIds(syncQuery)
      if (!activeSyncIds.isEmpty()) {
        val scopedConfigurationsForWorkspace =
          actorIdsForWorkspace
            .asSequence()
            .map { key: UUID -> scopedConfigurationByActorId[key] }
            .filterNotNull()
            .toList()
        returnValue.add(WorkspaceBreakingChangeInfo(workspaceId, activeSyncIds, scopedConfigurationsForWorkspace))
      }
    }

    return returnValue
  }

  @Throws(IOException::class)
  private fun getActorIdsByWorkspace(
    actorType: ActorType,
    scopedConfigs: Collection<ScopedConfiguration>,
  ): Map<UUID, List<UUID>> {
    val actorIds = scopedConfigs.map { it.scopeId }

    return when (actorType) {
      ActorType.SOURCE ->
        sourceService
          .listSourcesWithIds(actorIds)
          .groupBy { it.workspaceId }
          .mapValues { entry -> entry.value.map { it.sourceId } }

      ActorType.DESTINATION ->
        destinationService
          .listDestinationsWithIds(actorIds)
          .groupBy { it.workspaceId }
          .mapValues { entry -> entry.value.map { it.destinationId } }

      else -> throw IllegalArgumentException("Actor type not supported: $actorType")
    }
  }

  private fun buildStandardSyncQuery(
    actorType: ActorType,
    workspaceId: UUID,
    actorIds: List<UUID>,
  ): StandardSyncQuery =
    when (actorType) {
      ActorType.SOURCE -> {
        StandardSyncQuery(workspaceId, actorIds, null, false)
      }

      ActorType.DESTINATION -> {
        StandardSyncQuery(workspaceId, null, actorIds, false)
      }

      else -> throw IllegalArgumentException("Actor type not supported: $actorType")
    }

  companion object {
    /**
     * Given a list of breaking changes and the current default version, filter for breaking changes
     * that would affect the current default version. This is used to avoid considering breaking changes
     * that may have been rolled back.
     *
     * @param breakingChanges - breaking changes for a definition
     * @param currentDefaultVersion - current default version for a definition
     * @return filtered breaking changes
     */
    fun filterApplicableBreakingChanges(
      breakingChanges: List<ActorDefinitionBreakingChange>,
      currentDefaultVersion: Version,
    ): List<ActorDefinitionBreakingChange> =
      breakingChanges
        .stream()
        .filter { breakingChange: ActorDefinitionBreakingChange -> currentDefaultVersion.greaterThanOrEqualTo(breakingChange.version) }
        .toList()

    /**
     * Given a list of breaking changes for a definition, find the last applicable breaking change. In
     * this context, "Last" is defined as the breaking change with the highest version number.
     *
     * @param actorDefinitionService - actor definition service
     * @param defaultActorDefinitionVersionId - default version id for the definition
     * @param breakingChangesForDefinition - all breaking changes for the definition
     * @return last applicable breaking change
     */
    @JvmStatic
    @Throws(IOException::class, ConfigNotFoundException::class)
    fun getLastApplicableBreakingChange(
      actorDefinitionService: ActorDefinitionService,
      defaultActorDefinitionVersionId: UUID,
      breakingChangesForDefinition: List<ActorDefinitionBreakingChange>,
    ): ActorDefinitionBreakingChange {
      val defaultVersion = actorDefinitionService.getActorDefinitionVersion(defaultActorDefinitionVersionId)
      val currentDefaultVersion = Version(defaultVersion.dockerImageTag)

      return filterApplicableBreakingChanges(breakingChangesForDefinition, currentDefaultVersion)
        .stream()
        .max { v1: ActorDefinitionBreakingChange, v2: ActorDefinitionBreakingChange ->
          v1.version.versionCompareTo(
            v2.version,
          )
        }.orElseThrow()
    }
  }
}
