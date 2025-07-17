/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConnectionSummary
import io.airbyte.config.StandardSync
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.StreamDescriptorForDestination
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.shared.ConnectionWithJobInfo
import io.airbyte.data.services.shared.Filters
import io.airbyte.data.services.shared.SortKey
import io.airbyte.data.services.shared.StandardSyncQuery
import io.airbyte.data.services.shared.StandardSyncsQueryPaginated
import io.airbyte.data.services.shared.WorkspaceResourceCursorPagination
import io.airbyte.validation.json.JsonValidationException
import java.io.IOException
import java.util.UUID

/**
 * This service is used to manage connections.
 */
interface ConnectionService {
  @Throws(IOException::class)
  fun deleteStandardSync(syncId: UUID)

  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun getStandardSync(connectionId: UUID): StandardSync

  @Throws(IOException::class)
  fun writeStandardSync(standardSync: StandardSync)

  @Throws(IOException::class)
  fun listStandardSyncs(): List<StandardSync>

  @Throws(IOException::class)
  fun listStandardSyncsUsingOperation(operationId: UUID): List<StandardSync>

  @Throws(IOException::class)
  fun listWorkspaceStandardSyncs(
    workspaceId: UUID,
    includeDeleted: Boolean,
  ): List<StandardSync>

  @Throws(IOException::class)
  fun listWorkspaceStandardSyncs(standardSyncQuery: StandardSyncQuery): List<StandardSync>

  @Throws(IOException::class)
  fun listWorkspaceStandardSyncsCursorPaginated(
    standardSyncQuery: StandardSyncQuery,
    workspaceResourceCursorPagination: WorkspaceResourceCursorPagination,
  ): List<ConnectionWithJobInfo>

  @Throws(IOException::class)
  fun countWorkspaceStandardSyncs(
    standardSyncQuery: StandardSyncQuery,
    filters: Filters?,
  ): Int

  @Throws(IOException::class)
  fun listWorkspaceStandardSyncsLimitOffsetPaginated(
    workspaceIds: List<UUID>,
    tagIds: List<UUID>,
    includeDeleted: Boolean,
    pageSize: Int,
    rowOffset: Int,
  ): Map<UUID, List<StandardSync>>

  @Throws(IOException::class, ConfigNotFoundException::class, JsonValidationException::class)
  fun buildCursorPagination(
    cursor: UUID?,
    internalSortKey: SortKey,
    filters: Filters?,
    query: StandardSyncQuery?,
    ascending: Boolean?,
    pageSize: Int?,
  ): WorkspaceResourceCursorPagination?

  @Throws(IOException::class)
  fun listWorkspaceStandardSyncsLimitOffsetPaginated(standardSyncsQueryPaginated: StandardSyncsQueryPaginated): Map<UUID, List<StandardSync>>

  @Throws(IOException::class)
  fun listConnectionsBySource(
    sourceId: UUID,
    includeDeleted: Boolean,
  ): List<StandardSync>

  @Throws(IOException::class)
  fun listConnectionsByDestination(
    destinationId: UUID,
    includeDeleted: Boolean,
  ): List<StandardSync>

  @Throws(IOException::class)
  fun listConnectionsBySources(
    sourceIds: List<UUID>,
    includeDeleted: Boolean,
    includeInactive: Boolean,
  ): List<StandardSync>

  @Throws(IOException::class)
  fun listConnectionsByDestinations(
    destinationIds: List<UUID>,
    includeDeleted: Boolean,
    includeInactive: Boolean,
  ): List<StandardSync>

  @Throws(IOException::class)
  fun listConnectionsByActorDefinitionIdAndType(
    actorDefinitionId: UUID,
    actorTypeValue: String,
    includeDeleted: Boolean,
    includeInactive: Boolean,
  ): List<StandardSync>

  @Throws(IOException::class)
  fun listConnectionSummaryByActorDefinitionIdAndActorIds(
    actorDefinitionId: UUID,
    actorTypeValue: String,
    actorIds: List<UUID>,
  ): List<ConnectionSummary>

  @Throws(ConfigNotFoundException::class, IOException::class)
  fun getAllStreamsForConnection(connectionId: UUID): List<StreamDescriptor>

  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun getConfiguredCatalogForConnection(connectionId: UUID): ConfiguredAirbyteCatalog

  @Throws(IOException::class)
  fun getDataplaneGroupNameForConnection(connectionId: UUID): String

  @Throws(IOException::class)
  fun getConnectionHasAlphaOrBetaConnector(connectionId: UUID): Boolean

  @Throws(IOException::class)
  fun actorSyncsAnyListedStream(
    actorID: UUID,
    streamNames: List<String>,
  ): Boolean

  @Throws(IOException::class)
  fun listEarlySyncJobs(
    freeUsageInterval: Int,
    jobsFetchRange: Int,
  ): Set<Long>

  @Throws(IOException::class)
  fun disableConnectionsById(connectionIds: List<UUID>): Set<UUID>

  @Throws(IOException::class)
  fun listConnectionIdsForWorkspace(workspaceId: UUID): List<UUID>

  @Throws(IOException::class)
  fun listConnectionIdsForOrganization(organizationId: UUID): List<UUID>

  @Throws(IOException::class)
  fun listStreamsForDestination(
    destinationId: UUID,
    connectionId: UUID?,
  ): List<StreamDescriptorForDestination>

  /**
   * Get aggregated connection status counts for a workspace.
   *
   * @param workspaceId workspace id
   * @return connection status counts
   * @throws IOException if there is an issue while interacting with db.
   */
  @Throws(IOException::class)
  fun getConnectionStatusCounts(workspaceId: UUID): ConnectionStatusCounts

  /**
   * Record representing connection status counts for a workspace.
   */
  @JvmRecord
  data class ConnectionStatusCounts(
    val running: Int,
    val healthy: Int,
    val failed: Int,
    val paused: Int,
    val notSynced: Int,
  )
}
