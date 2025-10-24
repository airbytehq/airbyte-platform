/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.ActorType
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConnectionSummary
import io.airbyte.config.StandardSync
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.StreamDescriptorForDestination
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.shared.ConnectionCronSchedule
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
  fun deleteStandardSync(syncId: UUID)

  fun getStandardSync(connectionId: UUID): StandardSync

  fun writeStandardSync(standardSync: StandardSync)

  fun listStandardSyncs(): List<StandardSync>

  fun listStandardSyncsUsingOperation(operationId: UUID): List<StandardSync>

  fun listWorkspaceStandardSyncs(
    workspaceId: UUID,
    includeDeleted: Boolean,
  ): List<StandardSync>

  fun listWorkspaceStandardSyncs(standardSyncQuery: StandardSyncQuery): List<StandardSync>

  fun listWorkspaceStandardSyncsCursorPaginated(
    standardSyncQuery: StandardSyncQuery,
    workspaceResourceCursorPagination: WorkspaceResourceCursorPagination,
  ): List<ConnectionWithJobInfo>

  fun countWorkspaceStandardSyncs(
    standardSyncQuery: StandardSyncQuery,
    filters: Filters?,
  ): Int

  fun listWorkspaceStandardSyncsLimitOffsetPaginated(
    workspaceIds: List<UUID>,
    tagIds: List<UUID>,
    includeDeleted: Boolean,
    pageSize: Int,
    rowOffset: Int,
  ): Map<UUID, List<StandardSync>>

  fun buildCursorPagination(
    cursor: UUID?,
    internalSortKey: SortKey,
    filters: Filters?,
    query: StandardSyncQuery?,
    ascending: Boolean?,
    pageSize: Int?,
  ): WorkspaceResourceCursorPagination?

  fun listWorkspaceStandardSyncsLimitOffsetPaginated(standardSyncsQueryPaginated: StandardSyncsQueryPaginated): Map<UUID, List<StandardSync>>

  fun listConnectionsBySource(
    sourceId: UUID,
    includeDeleted: Boolean,
  ): List<StandardSync>

  fun listConnectionsByDestination(
    destinationId: UUID,
    includeDeleted: Boolean,
  ): List<StandardSync>

  fun updateConnectionStatus(
    connectionId: UUID,
    status: StandardSync.Status,
    statusReason: String? = null,
  )

  fun listConnectionsBySources(
    sourceIds: List<UUID>,
    includeDeleted: Boolean,
    includeInactive: Boolean,
  ): List<StandardSync>

  fun listConnectionsByDestinations(
    destinationIds: List<UUID>,
    includeDeleted: Boolean,
    includeInactive: Boolean,
  ): List<StandardSync>

  fun listConnectionsByActorDefinitionIdAndType(
    actorDefinitionId: UUID,
    actorTypeValue: String,
    includeDeleted: Boolean,
    includeInactive: Boolean,
  ): List<StandardSync>

  fun listConnectionSummaryByActorDefinitionIdAndActorIds(
    actorDefinitionId: UUID,
    actorTypeValue: String,
    actorIds: List<UUID>,
  ): List<ConnectionSummary>

  fun getAllStreamsForConnection(connectionId: UUID): List<StreamDescriptor>

  fun getConfiguredCatalogForConnection(connectionId: UUID): ConfiguredAirbyteCatalog

  fun getDataplaneGroupNameForConnection(connectionId: UUID): String

  fun getConnectionHasAlphaOrBetaConnector(connectionId: UUID): Boolean

  fun actorSyncsAnyListedStream(
    actorID: UUID,
    streamNames: List<String>,
  ): Boolean

  fun listEarlySyncJobs(
    freeUsageInterval: Int,
    jobsFetchRange: Int,
  ): Set<Long>

  fun disableConnectionsById(connectionIds: List<UUID>): Set<UUID>

  fun lockConnectionsById(
    connectionIds: Collection<UUID>,
    statusReason: String,
  ): Set<UUID>

  fun listConnectionIdsForWorkspace(workspaceId: UUID): List<UUID>

  fun listConnectionIdsForOrganization(organizationId: UUID): List<UUID>

  fun listConnectionIdsForOrganizationAndActorDefinitions(
    organizationId: UUID,
    actorDefinitionIds: Collection<UUID>,
    actorType: ActorType,
  ): List<UUID>

  fun listConnectionIdsForOrganizationWithMappers(organizationId: UUID): List<UUID>

  fun listSubHourConnectionIdsForOrganization(organizationId: UUID): List<UUID>

  fun listConnectionCronSchedulesForOrganization(organizationId: UUID): List<ConnectionCronSchedule>

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
