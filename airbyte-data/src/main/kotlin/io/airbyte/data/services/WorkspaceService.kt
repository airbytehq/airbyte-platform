/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.StandardWorkspace
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.shared.ResourcesQueryPaginated
import io.airbyte.data.services.shared.StandardSyncQuery
import io.airbyte.validation.json.JsonValidationException
import java.io.IOException
import java.util.Optional
import java.util.UUID
import java.util.stream.Stream

/**
 * A service that manages workspaces.
 */
interface WorkspaceService {
  fun getStandardWorkspaceNoSecrets(
    workspaceId: UUID,
    includeTombstone: Boolean,
  ): StandardWorkspace

  fun getWorkspaceBySlugOptional(
    slug: String,
    includeTombstone: Boolean,
  ): Optional<StandardWorkspace>

  fun getWorkspaceBySlug(
    slug: String,
    includeTombstone: Boolean,
  ): StandardWorkspace

  fun listStandardWorkspaces(includeTombstone: Boolean): List<StandardWorkspace>

  fun listWorkspaceQuery(
    workspaceId: Optional<List<UUID>>,
    includeTombstone: Boolean,
  ): Stream<StandardWorkspace>

  fun listStandardWorkspacesPaginated(resourcesQueryPaginated: ResourcesQueryPaginated): List<StandardWorkspace>

  fun getStandardWorkspaceFromConnection(
    connectionId: UUID,
    isTombstone: Boolean,
  ): StandardWorkspace

  fun writeStandardWorkspaceNoSecrets(workspace: StandardWorkspace)

  fun setFeedback(workspaceId: UUID)

  fun workspaceCanUseDefinition(
    actorDefinitionId: UUID,
    workspaceId: UUID,
  ): Boolean

  fun workspaceCanUseCustomDefinition(
    actorDefinitionId: UUID,
    workspaceId: UUID,
  ): Boolean

  fun listActiveWorkspacesByMostRecentlyRunningJobs(timeWindowInHours: Int): List<UUID>

  fun countConnectionsForWorkspace(workspaceId: UUID): Int

  fun countSourcesForWorkspace(workspaceId: UUID): Int

  fun countDestinationsForWorkspace(workspaceId: UUID): Int

  fun getWorkspaceHasAlphaOrBetaConnector(workspaceId: UUID): Boolean

  fun listWorkspaceActiveSyncIds(standardSyncQuery: StandardSyncQuery): List<UUID>

  fun listStandardWorkspacesWithIds(
    workspaceIds: List<UUID>,
    includeTombstone: Boolean,
  ): List<StandardWorkspace>

  fun getOrganizationIdFromWorkspaceId(scopeId: UUID?): Optional<UUID>

  fun getWorkspaceWithSecrets(
    workspaceId: UUID,
    includeTombstone: Boolean,
  ): StandardWorkspace

  fun writeWorkspaceWithSecrets(workspace: StandardWorkspace)
}
