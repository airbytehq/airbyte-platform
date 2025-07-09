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
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun getStandardWorkspaceNoSecrets(
    workspaceId: UUID,
    includeTombstone: Boolean,
  ): StandardWorkspace

  @Throws(IOException::class)
  fun getWorkspaceBySlugOptional(
    slug: String,
    includeTombstone: Boolean,
  ): Optional<StandardWorkspace>

  @Throws(IOException::class, ConfigNotFoundException::class)
  fun getWorkspaceBySlug(
    slug: String,
    includeTombstone: Boolean,
  ): StandardWorkspace

  @Throws(IOException::class)
  fun listStandardWorkspaces(includeTombstone: Boolean): List<StandardWorkspace>

  @Throws(IOException::class)
  fun listWorkspaceQuery(
    workspaceId: Optional<List<UUID>>,
    includeTombstone: Boolean,
  ): Stream<StandardWorkspace>

  @Throws(IOException::class)
  fun listStandardWorkspacesPaginated(resourcesQueryPaginated: ResourcesQueryPaginated): List<StandardWorkspace>

  @Throws(ConfigNotFoundException::class)
  fun getStandardWorkspaceFromConnection(
    connectionId: UUID,
    isTombstone: Boolean,
  ): StandardWorkspace

  @Throws(JsonValidationException::class, IOException::class)
  fun writeStandardWorkspaceNoSecrets(workspace: StandardWorkspace)

  @Throws(IOException::class, ConfigNotFoundException::class)
  fun setFeedback(workspaceId: UUID)

  @Throws(IOException::class)
  fun workspaceCanUseDefinition(
    actorDefinitionId: UUID,
    workspaceId: UUID,
  ): Boolean

  @Throws(IOException::class)
  fun workspaceCanUseCustomDefinition(
    actorDefinitionId: UUID,
    workspaceId: UUID,
  ): Boolean

  @Throws(IOException::class)
  fun listActiveWorkspacesByMostRecentlyRunningJobs(timeWindowInHours: Int): List<UUID>

  @Throws(IOException::class)
  fun countConnectionsForWorkspace(workspaceId: UUID): Int

  @Throws(IOException::class)
  fun countSourcesForWorkspace(workspaceId: UUID): Int

  @Throws(IOException::class)
  fun countDestinationsForWorkspace(workspaceId: UUID): Int

  @Throws(IOException::class)
  fun getWorkspaceHasAlphaOrBetaConnector(workspaceId: UUID): Boolean

  @Throws(IOException::class)
  fun listWorkspaceActiveSyncIds(standardSyncQuery: StandardSyncQuery): List<UUID>

  @Throws(IOException::class)
  fun listStandardWorkspacesWithIds(
    workspaceIds: List<UUID>,
    includeTombstone: Boolean,
  ): List<StandardWorkspace>

  @Throws(IOException::class)
  fun getOrganizationIdFromWorkspaceId(scopeId: UUID?): Optional<UUID>

  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun getWorkspaceWithSecrets(
    workspaceId: UUID,
    includeTombstone: Boolean,
  ): StandardWorkspace

  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun writeWorkspaceWithSecrets(workspace: StandardWorkspace)
}
