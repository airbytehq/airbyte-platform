/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.config.Permission
import io.airbyte.config.ScopeType
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.Tag
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.config.persistence.WorkspacePersistence
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.ConnectorBuilderService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.OAuthService
import io.airbyte.data.services.OperationService
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.TagService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.domain.models.OrganizationId
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

private val logger = KotlinLogging.logger {}

/**
 * Merges an organization's workspaces down to its plan's workspace limit. The oldest workspaces are kept, matching
 * [WorkspacePersistence.getDefaultWorkspaceForOrganization], and everything in the newer ones is moved into the
 * oldest one: sources and destinations (connections follow them), operations, tags, custom connector grants,
 * OAuth overrides, builder projects, permissions, and users' default workspace. Each emptied workspace is
 * tombstoned only after all of its content has moved, so a failed run can be repeated.
 *
 * Connections are moved even when the workspaces differ in region. The change is logged with the affected
 * connection ids and recorded in the returned summary.
 */
@Singleton
internal class WorkspaceConsolidationStep(
  private val workspacePersistence: WorkspacePersistence,
  private val workspaceService: WorkspaceService,
  private val sourceService: SourceService,
  private val destinationService: DestinationService,
  private val connectionService: ConnectionService,
  private val operationService: OperationService,
  private val tagService: TagService,
  private val permissionService: PermissionService,
  private val actorDefinitionService: ActorDefinitionService,
  private val oAuthService: OAuthService,
  private val connectorBuilderService: ConnectorBuilderService,
  private val userPersistence: UserPersistence,
) {
  fun run(
    organizationId: OrganizationId,
    workspaceLimit: Long,
  ): PlanLimitStepResult {
    val workspaces = workspacePersistence.listWorkspacesByOrganizationId(organizationId.value, false, Optional.empty())
    if (workspaces.size <= workspaceLimit) {
      return PlanLimitStepResult.NotNeeded
    }

    val kept = workspacePersistence.getDefaultWorkspaceForOrganization(organizationId.value)
    val toMerge =
      workspaces
        .filter { it.workspaceId != kept.workspaceId }
        .sortedWith(compareBy({ it.createdAt }, { it.workspaceId }))
        .drop((workspaceLimit - 1).toInt().coerceAtLeast(0))

    logger.info {
      "Consolidating workspaces for organization over its plan limit. organizationId=$organizationId limit=$workspaceLimit " +
        "keptWorkspaceId=${kept.workspaceId} mergedWorkspaceIds=${toMerge.map { it.workspaceId }}"
    }

    val movedConnectionIds = mutableListOf<UUID>()
    val regionChangedConnectionIds = mutableListOf<UUID>()
    toMerge.forEach { extra ->
      val connectionIds = mergeInto(organizationId, extra, kept)
      movedConnectionIds += connectionIds
      if (extra.dataplaneGroupId != kept.dataplaneGroupId) {
        regionChangedConnectionIds += connectionIds
      }
    }

    return PlanLimitStepResult.Completed(
      "merged ${toMerge.size} workspace(s) into ${kept.workspaceId}, moved ${movedConnectionIds.size} connection(s), " +
        "${regionChangedConnectionIds.size} changed region",
    )
  }

  /**
   * Moves everything in [extra] into [kept] and tombstones [extra]. Returns the ids of the connections that moved.
   */
  private fun mergeInto(
    organizationId: OrganizationId,
    extra: StandardWorkspace,
    kept: StandardWorkspace,
  ): List<UUID> {
    val connections = connectionService.listWorkspaceStandardSyncs(extra.workspaceId, false)
    val connectionIds = connections.map { it.connectionId }

    if (extra.dataplaneGroupId != kept.dataplaneGroupId) {
      logger.warn {
        "Workspace ${extra.workspaceId} (dataplane group ${extra.dataplaneGroupId}) is being merged into workspace ${kept.workspaceId} " +
          "(dataplane group ${kept.dataplaneGroupId}). These connections will run in the new region: $connectionIds organizationId=$organizationId"
      }
    }

    // Grants and overrides first so the moved actors resolve their definitions and credentials in the kept workspace.
    moveCustomConnectorGrants(extra, kept)
    oAuthService.reassignWorkspaceOAuthParams(extra.workspaceId, kept.workspaceId)

    sourceService.listWorkspaceSourceConnection(extra.workspaceId).forEach {
      sourceService.writeSourceConnectionNoSecrets(it.withWorkspaceId(kept.workspaceId))
    }
    destinationService.listWorkspaceDestinationConnection(extra.workspaceId).forEach {
      destinationService.writeDestinationConnectionNoSecrets(it.withWorkspaceId(kept.workspaceId))
    }

    moveOperations(connections, kept)
    // Tags are validated against the connection's workspace, so this must run after the actors have moved.
    moveTags(extra, kept, connections)
    connectorBuilderService.reassignWorkspaceBuilderProjects(extra.workspaceId, kept.workspaceId)
    movePermissions(extra, kept)
    userPersistence.reassignDefaultWorkspace(extra.workspaceId, kept.workspaceId)

    workspaceService.writeStandardWorkspaceNoSecrets(
      workspaceService.getStandardWorkspaceNoSecrets(extra.workspaceId, true).withTombstone(true),
    )
    logger.info { "Merged workspace ${extra.workspaceId} into ${kept.workspaceId}. organizationId=$organizationId connectionIds=$connectionIds" }
    return connectionIds
  }

  private fun moveCustomConnectorGrants(
    extra: StandardWorkspace,
    kept: StandardWorkspace,
  ) {
    val definitionIds =
      sourceService.listGrantedSourceDefinitions(extra.workspaceId, true).map { it.sourceDefinitionId } +
        destinationService.listGrantedDestinationDefinitions(extra.workspaceId, true).map { it.destinationDefinitionId }
    definitionIds.forEach { definitionId ->
      if (!actorDefinitionService.actorDefinitionWorkspaceGrantExists(definitionId, kept.workspaceId, ScopeType.WORKSPACE)) {
        actorDefinitionService.writeActorDefinitionWorkspaceGrant(definitionId, kept.workspaceId, ScopeType.WORKSPACE)
      }
      actorDefinitionService.deleteActorDefinitionWorkspaceGrant(definitionId, extra.workspaceId, ScopeType.WORKSPACE)
    }
  }

  private fun moveOperations(
    connections: List<StandardSync>,
    kept: StandardWorkspace,
  ) {
    connections
      .flatMap { it.operationIds ?: emptyList() }
      .distinct()
      .forEach { operationId ->
        val operation = operationService.getStandardSyncOperation(operationId)
        if (operation.workspaceId != kept.workspaceId) {
          operationService.writeStandardSyncOperation(operation.withWorkspaceId(kept.workspaceId))
        }
      }
  }

  private fun moveTags(
    extra: StandardWorkspace,
    kept: StandardWorkspace,
    connections: List<StandardSync>,
  ) {
    val extraTags = tagService.getTagsByWorkspaceIds(listOf(extra.workspaceId))
    if (extraTags.isEmpty()) {
      return
    }

    val keptTagsByName = tagService.getTagsByWorkspaceIds(listOf(kept.workspaceId)).associateBy { it.name }.toMutableMap()
    val replacements: Map<UUID, Tag?> =
      extraTags.associate { old ->
        val replacement =
          keptTagsByName[old.name]
            ?: try {
              tagService
                .createTag(
                  Tag()
                    .withTagId(UUID.randomUUID())
                    .withWorkspaceId(kept.workspaceId)
                    .withName(old.name)
                    .withColor(old.color),
                ).also { keptTagsByName[it.name] = it }
            } catch (e: IllegalStateException) {
              logger.warn(
                e,
              ) { "Dropping tag '${old.name}' while merging workspace ${extra.workspaceId}: workspace ${kept.workspaceId} is at its tag limit" }
              null
            }
        old.tagId to replacement
      }

    connections.forEach { moved ->
      val current = connectionService.getStandardSync(moved.connectionId)
      val currentTags = current.tags ?: emptyList()
      if (currentTags.none { it.workspaceId == extra.workspaceId }) {
        return@forEach
      }
      val newTags =
        currentTags
          .mapNotNull { tag -> if (tag.workspaceId == extra.workspaceId) replacements[tag.tagId] else tag }
          .distinctBy { it.tagId }
      connectionService.writeStandardSync(current.withTags(newTags))
    }

    extraTags.forEach { tagService.deleteTag(it.tagId) }
  }

  private fun movePermissions(
    extra: StandardWorkspace,
    kept: StandardWorkspace,
  ) {
    val extraPermissions = permissionService.getPermissionsByWorkspaceId(extra.workspaceId)
    if (extraPermissions.isEmpty()) {
      return
    }
    val usersOnKept = permissionService.getPermissionsByWorkspaceId(kept.workspaceId).mapNotNull { it.userId }.toSet()
    val (duplicates, toMove) = extraPermissions.partition { it.userId != null && it.userId in usersOnKept }

    if (duplicates.isNotEmpty()) {
      permissionService.deletePermissions(duplicates.map(Permission::getPermissionId))
    }
    if (toMove.isNotEmpty()) {
      toMove.forEach { it.workspaceId = kept.workspaceId }
      permissionService.updatePermissions(toMove)
    }
  }
}
