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
  fun getStandardSourceDefinition(sourceDefinitionId: UUID): StandardSourceDefinition

  fun getStandardSourceDefinition(
    sourceDefinitionId: UUID,
    includeTombstone: Boolean,
  ): StandardSourceDefinition

  fun getSourceDefinitionFromSource(sourceId: UUID): StandardSourceDefinition

  fun getSourceDefinitionFromConnection(connectionId: UUID): StandardSourceDefinition

  fun listStandardSourceDefinitions(includeTombstone: Boolean): List<StandardSourceDefinition>

  fun listPublicSourceDefinitions(includeTombstone: Boolean): List<StandardSourceDefinition>

  fun listSourceDefinitionsForWorkspace(
    workspaceId: UUID,
    includeTombstone: Boolean,
  ): List<StandardSourceDefinition>

  fun listGrantedSourceDefinitions(
    workspaceId: UUID,
    includeTombstones: Boolean,
  ): List<StandardSourceDefinition>

  fun listGrantableSourceDefinitions(
    workspaceId: UUID,
    includeTombstones: Boolean,
  ): List<Map.Entry<StandardSourceDefinition, Boolean>>

  fun updateStandardSourceDefinition(sourceDefinition: StandardSourceDefinition)

  fun getSourceConnection(sourceId: UUID): SourceConnection

  fun getSourceConnectionIfExists(sourceId: UUID): Optional<SourceConnection>

  fun listSourceConnection(): List<SourceConnection>

  fun listWorkspaceSourceConnection(workspaceId: UUID): List<SourceConnection>

  fun isSourceActive(sourceId: UUID): Boolean

  fun listWorkspacesSourceConnections(resourcesQueryPaginated: ResourcesQueryPaginated): List<SourceConnection>

  fun listSourcesForDefinition(definitionId: UUID): List<SourceConnection>

  fun getSourceAndDefinitionsFromSourceIds(sourceIds: List<UUID>): List<SourceAndDefinition>

  fun listSourcesWithIds(sourceIds: List<UUID>): List<SourceConnection>

  fun writeConnectorMetadata(
    sourceDefinition: StandardSourceDefinition,
    actorDefinitionVersion: ActorDefinitionVersion,
    breakingChangesForDefinition: List<ActorDefinitionBreakingChange>,
  )

  fun writeCustomConnectorMetadata(
    sourceDefinition: StandardSourceDefinition,
    defaultVersion: ActorDefinitionVersion,
    scopeId: UUID,
    scopeType: ScopeType,
  )

  fun writeSourceConnectionNoSecrets(partialSource: SourceConnection)

  fun tombstoneSource(
    name: String,
    workspaceId: UUID,
    sourceId: UUID,
  )

  fun listWorkspaceSourceConnectionsWithCounts(
    workspaceId: UUID,
    workspaceResourceCursorPagination: WorkspaceResourceCursorPagination,
  ): List<SourceConnectionWithCount>

  fun countWorkspaceSourcesFiltered(
    workspaceId: UUID,
    workspaceResourceCursorPagination: WorkspaceResourceCursorPagination,
  ): Int

  fun buildCursorPagination(
    cursor: UUID?,
    internalSortKey: SortKey,
    filters: Filters?,
    ascending: Boolean?,
    pageSize: Int?,
  ): WorkspaceResourceCursorPagination?
}
