/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import com.google.common.annotations.VisibleForTesting
import io.airbyte.config.ConfigScopeType
import io.airbyte.config.persistence.WorkspacePersistence
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import jakarta.inject.Inject
import java.util.Optional
import java.util.UUID
import kotlin.jvm.optionals.getOrNull

class ScopedConfigurationRelationshipResolver
  @Inject
  constructor(
    private val workspaceService: WorkspaceService,
    private val workspacePersistence: WorkspacePersistence,
    private val sourceService: SourceService,
    private val destinationService: DestinationService,
  ) {
    private fun getOrganizationChildScopeIds(
      childScopeType: ConfigScopeType,
      organizationId: UUID,
    ): List<UUID> =
      when (childScopeType) {
        ConfigScopeType.WORKSPACE ->
          workspacePersistence
            .listWorkspacesByOrganizationId(
              organizationId,
              true,
              Optional.empty(),
            ).map { it.workspaceId }
        else -> throw IllegalArgumentException("Unsupported child scope type for organization: $childScopeType")
      }

    private fun getWorkspaceParentScopeId(
      parentScopeType: ConfigScopeType,
      workspaceId: UUID,
    ): UUID? =
      when (parentScopeType) {
        ConfigScopeType.ORGANIZATION -> workspaceService.getOrganizationIdFromWorkspaceId(workspaceId).getOrNull()
        else -> throw IllegalArgumentException("Unsupported parent scope type for workspace: $parentScopeType")
      }

    private fun getWorkspaceChildScopeIds(
      childScopeType: ConfigScopeType,
      workspaceId: UUID,
    ): List<UUID> =
      when (childScopeType) {
        ConfigScopeType.ACTOR -> {
          sourceService.listWorkspaceSourceConnection(workspaceId).map { it.sourceId } +
            destinationService.listWorkspaceDestinationConnection(workspaceId).map { it.destinationId }
        }
        else -> throw IllegalArgumentException("Unsupported child scope type for workspace: $childScopeType")
      }

    private fun getActorParentScopeId(
      parentScopeType: ConfigScopeType,
      actorId: UUID,
    ): UUID? =
      when (parentScopeType) {
        ConfigScopeType.WORKSPACE ->
          try {
            sourceService.getSourceConnection(actorId).workspaceId
          } catch (e: ConfigNotFoundException) {
            destinationService.getDestinationConnection(actorId).workspaceId
          }
        else -> throw IllegalArgumentException("Unsupported parent scope type for actor: $parentScopeType")
      }

    /**
     * Returns the parent scope of the given type for the given scope.
     */
    @VisibleForTesting
    fun getParentScopeId(
      scopeType: ConfigScopeType,
      parentType: ConfigScopeType,
      scopeId: UUID,
    ): UUID? =
      when (scopeType) {
        ConfigScopeType.WORKSPACE -> getWorkspaceParentScopeId(parentType, scopeId)
        ConfigScopeType.ACTOR -> getActorParentScopeId(parentType, scopeId)
        else -> throw IllegalArgumentException("Unsupported scope type to get parent: $scopeType")
      }

    /**
     * Returns the child scopes of the given type for the given scope.
     */
    @VisibleForTesting
    fun getChildScopeIds(
      scopeType: ConfigScopeType,
      childType: ConfigScopeType,
      scopeId: UUID,
    ): List<UUID> =
      when (scopeType) {
        ConfigScopeType.ORGANIZATION -> getOrganizationChildScopeIds(childType, scopeId)
        ConfigScopeType.WORKSPACE -> getWorkspaceChildScopeIds(childType, scopeId)
        else -> throw IllegalArgumentException("Unsupported scope type to get child: $scopeType")
      }

    /**
     * Returns a map of all scopes that are ancestors of the given scope, according to the given scope priority.
     */
    fun getAllAncestorScopes(
      scopePriorityOrder: List<ConfigScopeType>,
      scopeType: ConfigScopeType,
      scopeId: UUID,
    ): Map<ConfigScopeType, UUID> {
      val currentScopePriorityIdx = scopePriorityOrder.indexOf(scopeType)

      if (currentScopePriorityIdx == -1) {
        throw IllegalArgumentException("Unsupported scope type: $scopeType")
      } else if (currentScopePriorityIdx == scopePriorityOrder.lastIndex) {
        return emptyMap()
      }

      val parentScopeType = scopePriorityOrder[currentScopePriorityIdx + 1]
      val parentScopeId = getParentScopeId(scopeType, parentScopeType, scopeId)
      return if (parentScopeId != null) {
        getAllAncestorScopes(scopePriorityOrder, parentScopeType, parentScopeId) + (parentScopeType to parentScopeId)
      } else {
        emptyMap()
      }
    }

    /**
     * Returns a map of all scopes that are descendants of the given scope, according to the given scope priority.
     */
    fun getAllDescendantScopes(
      scopePriorityOrder: List<ConfigScopeType>,
      scopeType: ConfigScopeType,
      scopeId: UUID,
    ): Map<ConfigScopeType, List<UUID>> {
      val currentScopePriorityIdx = scopePriorityOrder.indexOf(scopeType)

      if (currentScopePriorityIdx == -1) {
        throw IllegalArgumentException("Unsupported scope type: $scopeType")
      } else if (currentScopePriorityIdx == 0) {
        return emptyMap()
      }

      val childScopeType = scopePriorityOrder[currentScopePriorityIdx - 1]
      val childScopeIds = getChildScopeIds(scopeType, childScopeType, scopeId)
      return if (childScopeIds.isNotEmpty()) {
        // Initialize the map with the direct child scopes
        val resultMap = mutableMapOf(childScopeType to childScopeIds)

        // Recursively get all descendant scopes and merge them into the resultMap
        childScopeIds.forEach { childScopeId ->
          getAllDescendantScopes(scopePriorityOrder, childScopeType, childScopeId).forEach { (descendantType, descendantIds) ->
            resultMap.merge(descendantType, descendantIds) { oldList, newList ->
              oldList + newList
            }
          }
        }

        resultMap
      } else {
        emptyMap()
      }
    }
  }
