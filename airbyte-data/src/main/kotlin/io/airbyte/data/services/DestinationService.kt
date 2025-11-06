/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.ActorDefinitionBreakingChange
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.DestinationConnection
import io.airbyte.config.ScopeType
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.data.services.shared.DestinationAndDefinition
import io.airbyte.data.services.shared.DestinationConnectionWithCount
import io.airbyte.data.services.shared.Filters
import io.airbyte.data.services.shared.ResourcesQueryPaginated
import io.airbyte.data.services.shared.SortKey
import io.airbyte.data.services.shared.WorkspaceResourceCursorPagination
import java.util.Optional
import java.util.UUID

/**
 * This service is used to interact with destinations.
 */
interface DestinationService {
  fun getStandardDestinationDefinition(destinationDefinitionId: UUID): StandardDestinationDefinition

  fun getStandardDestinationDefinition(
    destinationDefinitionId: UUID,
    includeTombstone: Boolean,
  ): StandardDestinationDefinition

  fun getDestinationDefinitionFromDestination(destinationId: UUID): StandardDestinationDefinition

  fun isDestinationActive(destinationId: UUID): Boolean

  fun getDestinationDefinitionFromConnection(connectionId: UUID): StandardDestinationDefinition

  fun listStandardDestinationDefinitions(includeTombstone: Boolean): List<StandardDestinationDefinition>

  fun listPublicDestinationDefinitions(includeTombstone: Boolean): List<StandardDestinationDefinition>

  fun listDestinationDefinitionsForWorkspace(
    workspaceId: UUID,
    includeTombstone: Boolean,
  ): List<StandardDestinationDefinition>

  fun listGrantedDestinationDefinitions(
    workspaceId: UUID,
    includeTombstones: Boolean,
  ): List<StandardDestinationDefinition>

  fun listGrantableDestinationDefinitions(
    workspaceId: UUID,
    includeTombstones: Boolean,
  ): List<Map.Entry<StandardDestinationDefinition, Boolean>>

  fun updateStandardDestinationDefinition(destinationDefinition: StandardDestinationDefinition)

  fun getDestinationConnection(destinationId: UUID): DestinationConnection

  fun getDestinationConnectionIfExists(destinationId: UUID): Optional<DestinationConnection>

  fun writeDestinationConnectionNoSecrets(partialDestination: DestinationConnection)

  fun listDestinationConnection(): List<DestinationConnection>

  fun listWorkspaceDestinationConnection(workspaceId: UUID): List<DestinationConnection>

  fun listWorkspacesDestinationConnections(resourcesQueryPaginated: ResourcesQueryPaginated): List<DestinationConnection>

  fun listDestinationsForDefinition(definitionId: UUID): List<DestinationConnection>

  fun getDestinationAndDefinitionsFromDestinationIds(destinationIds: List<UUID>): List<DestinationAndDefinition>

  fun writeCustomConnectorMetadata(
    destinationDefinition: StandardDestinationDefinition,
    defaultVersion: ActorDefinitionVersion,
    scopeId: UUID,
    scopeType: ScopeType,
  )

  fun writeConnectorMetadata(
    destinationDefinition: StandardDestinationDefinition,
    actorDefinitionVersion: ActorDefinitionVersion,
    breakingChangesForDefinition: List<ActorDefinitionBreakingChange>,
  )

  fun listDestinationsWithIds(destinationIds: List<UUID>): List<DestinationConnection>

  fun tombstoneDestination(
    name: String,
    workspaceId: UUID,
    destinationId: UUID,
  )

  fun listWorkspaceDestinationConnectionsWithCounts(
    workspaceId: UUID,
    workspaceResourceCursorPagination: WorkspaceResourceCursorPagination,
  ): List<DestinationConnectionWithCount>

  fun countWorkspaceDestinationsFiltered(
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
