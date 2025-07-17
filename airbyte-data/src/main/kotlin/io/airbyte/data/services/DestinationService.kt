/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.ActorDefinitionBreakingChange
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.DestinationConnection
import io.airbyte.config.ScopeType
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.shared.DestinationAndDefinition
import io.airbyte.data.services.shared.DestinationConnectionWithCount
import io.airbyte.data.services.shared.Filters
import io.airbyte.data.services.shared.ResourcesQueryPaginated
import io.airbyte.data.services.shared.SortKey
import io.airbyte.data.services.shared.WorkspaceResourceCursorPagination
import io.airbyte.validation.json.JsonValidationException
import java.io.IOException
import java.util.Optional
import java.util.UUID

/**
 * This service is used to interact with destinations.
 */
interface DestinationService {
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun getStandardDestinationDefinition(destinationDefinitionId: UUID): StandardDestinationDefinition

  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun getStandardDestinationDefinition(
    destinationDefinitionId: UUID,
    includeTombstone: Boolean,
  ): StandardDestinationDefinition

  fun getDestinationDefinitionFromDestination(destinationId: UUID): StandardDestinationDefinition

  @Throws(IOException::class)
  fun isDestinationActive(destinationId: UUID): Boolean

  fun getDestinationDefinitionFromConnection(connectionId: UUID): StandardDestinationDefinition

  @Throws(IOException::class)
  fun listStandardDestinationDefinitions(includeTombstone: Boolean): List<StandardDestinationDefinition>

  @Throws(IOException::class)
  fun listPublicDestinationDefinitions(includeTombstone: Boolean): List<StandardDestinationDefinition>

  @Throws(IOException::class)
  fun listDestinationDefinitionsForWorkspace(
    workspaceId: UUID,
    includeTombstone: Boolean,
  ): List<StandardDestinationDefinition>

  @Throws(IOException::class)
  fun listGrantedDestinationDefinitions(
    workspaceId: UUID,
    includeTombstones: Boolean,
  ): List<StandardDestinationDefinition>

  @Throws(IOException::class)
  fun listGrantableDestinationDefinitions(
    workspaceId: UUID,
    includeTombstones: Boolean,
  ): List<Map.Entry<StandardDestinationDefinition, Boolean>>

  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun updateStandardDestinationDefinition(destinationDefinition: StandardDestinationDefinition)

  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun getDestinationConnection(destinationId: UUID): DestinationConnection

  fun getDestinationConnectionIfExists(destinationId: UUID): Optional<DestinationConnection>

  @Throws(IOException::class)
  fun writeDestinationConnectionNoSecrets(partialDestination: DestinationConnection)

  @Throws(IOException::class)
  fun listDestinationConnection(): List<DestinationConnection>

  @Throws(IOException::class)
  fun listWorkspaceDestinationConnection(workspaceId: UUID): List<DestinationConnection>

  @Throws(IOException::class)
  fun listWorkspacesDestinationConnections(resourcesQueryPaginated: ResourcesQueryPaginated): List<DestinationConnection>

  @Throws(IOException::class)
  fun listDestinationsForDefinition(definitionId: UUID): List<DestinationConnection>

  @Throws(IOException::class)
  fun getDestinationAndDefinitionsFromDestinationIds(destinationIds: List<UUID>): List<DestinationAndDefinition>

  @Throws(IOException::class)
  fun writeCustomConnectorMetadata(
    destinationDefinition: StandardDestinationDefinition,
    defaultVersion: ActorDefinitionVersion,
    scopeId: UUID,
    scopeType: ScopeType,
  )

  @Throws(IOException::class)
  fun writeConnectorMetadata(
    destinationDefinition: StandardDestinationDefinition,
    actorDefinitionVersion: ActorDefinitionVersion,
    breakingChangesForDefinition: List<ActorDefinitionBreakingChange>,
  )

  @Throws(IOException::class)
  fun listDestinationsWithIds(destinationIds: List<UUID>): List<DestinationConnection>

  @Throws(ConfigNotFoundException::class, JsonValidationException::class, IOException::class)
  fun tombstoneDestination(
    name: String,
    workspaceId: UUID,
    destinationId: UUID,
  )

  @Throws(IOException::class)
  fun listWorkspaceDestinationConnectionsWithCounts(
    workspaceId: UUID,
    workspaceResourceCursorPagination: WorkspaceResourceCursorPagination,
  ): List<DestinationConnectionWithCount>

  @Throws(IOException::class)
  fun countWorkspaceDestinationsFiltered(
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
