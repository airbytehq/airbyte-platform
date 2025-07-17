/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.ActorDefinitionBreakingChange
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ScopeType
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.shared.Filters
import io.airbyte.data.services.shared.ResourcesQueryPaginated
import io.airbyte.data.services.shared.SortKey
import io.airbyte.data.services.shared.SourceAndDefinition
import io.airbyte.data.services.shared.SourceConnectionWithCount
import io.airbyte.data.services.shared.WorkspaceResourceCursorPagination
import io.airbyte.validation.json.JsonValidationException
import java.io.IOException
import java.util.Optional
import java.util.UUID

/**
 * A service that manages sources.
 */
interface SourceService {
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun getStandardSourceDefinition(sourceDefinitionId: UUID): StandardSourceDefinition

  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun getStandardSourceDefinition(
    sourceDefinitionId: UUID,
    includeTombstone: Boolean,
  ): StandardSourceDefinition

  fun getSourceDefinitionFromSource(sourceId: UUID): StandardSourceDefinition

  fun getSourceDefinitionFromConnection(connectionId: UUID): StandardSourceDefinition

  @Throws(IOException::class)
  fun listStandardSourceDefinitions(includeTombstone: Boolean): List<StandardSourceDefinition>

  @Throws(IOException::class)
  fun listPublicSourceDefinitions(includeTombstone: Boolean): List<StandardSourceDefinition>

  @Throws(IOException::class)
  fun listSourceDefinitionsForWorkspace(
    workspaceId: UUID,
    includeTombstone: Boolean,
  ): List<StandardSourceDefinition>

  @Throws(IOException::class)
  fun listGrantedSourceDefinitions(
    workspaceId: UUID,
    includeTombstones: Boolean,
  ): List<StandardSourceDefinition>

  @Throws(IOException::class)
  fun listGrantableSourceDefinitions(
    workspaceId: UUID,
    includeTombstones: Boolean,
  ): List<Map.Entry<StandardSourceDefinition, Boolean>>

  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun updateStandardSourceDefinition(sourceDefinition: StandardSourceDefinition)

  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun getSourceConnection(sourceId: UUID): SourceConnection

  fun getSourceConnectionIfExists(sourceId: UUID): Optional<SourceConnection>

  @Throws(IOException::class)
  fun listSourceConnection(): List<SourceConnection>

  @Throws(IOException::class)
  fun listWorkspaceSourceConnection(workspaceId: UUID): List<SourceConnection>

  @Throws(IOException::class)
  fun isSourceActive(sourceId: UUID): Boolean

  @Throws(IOException::class)
  fun listWorkspacesSourceConnections(resourcesQueryPaginated: ResourcesQueryPaginated): List<SourceConnection>

  @Throws(IOException::class)
  fun listSourcesForDefinition(definitionId: UUID): List<SourceConnection>

  @Throws(IOException::class)
  fun getSourceAndDefinitionsFromSourceIds(sourceIds: List<UUID>): List<SourceAndDefinition>

  @Throws(IOException::class)
  fun listSourcesWithIds(sourceIds: List<UUID>): List<SourceConnection>

  @Throws(IOException::class)
  fun writeConnectorMetadata(
    sourceDefinition: StandardSourceDefinition,
    actorDefinitionVersion: ActorDefinitionVersion,
    breakingChangesForDefinition: List<ActorDefinitionBreakingChange>,
  )

  @Throws(IOException::class)
  fun writeCustomConnectorMetadata(
    sourceDefinition: StandardSourceDefinition,
    defaultVersion: ActorDefinitionVersion,
    scopeId: UUID,
    scopeType: ScopeType,
  )

  @Throws(IOException::class)
  fun writeSourceConnectionNoSecrets(partialSource: SourceConnection)

  @Throws(ConfigNotFoundException::class, JsonValidationException::class, IOException::class)
  fun tombstoneSource(
    name: String,
    workspaceId: UUID,
    sourceId: UUID,
  )

  @Throws(IOException::class)
  fun listWorkspaceSourceConnectionsWithCounts(
    workspaceId: UUID,
    workspaceResourceCursorPagination: WorkspaceResourceCursorPagination,
  ): List<SourceConnectionWithCount>

  @Throws(IOException::class)
  fun countWorkspaceSourcesFiltered(
    workspaceId: UUID,
    workspaceResourceCursorPagination: WorkspaceResourceCursorPagination,
  ): Int

  @Throws(IOException::class, ConfigNotFoundException::class, JsonValidationException::class)
  fun buildCursorPagination(
    cursor: UUID?,
    internalSortKey: SortKey,
    filters: Filters?,
    ascending: Boolean?,
    pageSize: Int?,
  ): WorkspaceResourceCursorPagination?
}
