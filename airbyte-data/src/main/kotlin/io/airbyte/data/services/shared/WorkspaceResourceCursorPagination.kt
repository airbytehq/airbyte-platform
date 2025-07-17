/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.shared

import com.google.common.annotations.VisibleForTesting
import io.airbyte.api.model.generated.ActorListFilters
import io.airbyte.api.model.generated.ActorListSortKey
import io.airbyte.api.model.generated.ActorStatus
import io.airbyte.api.model.generated.ActorType
import io.airbyte.api.model.generated.WebBackendConnectionListFilters
import io.airbyte.api.model.generated.WebBackendConnectionListSortKey
import java.util.UUID
import java.util.stream.Collectors

const val DEFAULT_PAGE_SIZE = 20

enum class SortKey {
  CONNECTION_NAME,
  SOURCE_NAME,
  SOURCE_DEFINITION_NAME,
  DESTINATION_NAME,
  DESTINATION_DEFINITION_NAME,
  LAST_SYNC,
}

enum class ConnectionJobStatus {
  HEALTHY,
  FAILED,
  RUNNING,
}

/**
 * Filter parameters for cursor-based queries.
 */
data class Filters(
  val searchTerm: String? = null,
  val sourceDefinitionIds: List<UUID>? = null,
  val destinationDefinitionIds: List<UUID>? = null,
  val statuses: List<ConnectionJobStatus>? = null,
  val states: List<ActorStatus>? = null,
  val tagIds: List<UUID>? = null,
)

data class Cursor(
  var sortKey: SortKey,
  var connectionName: String?,
  var sourceName: String?,
  var sourceDefinitionName: String?,
  var destinationName: String?,
  var destinationDefinitionName: String?,
  var lastSync: Long?,
  var cursorId: UUID?,
  var ascending: Boolean = true,
  var filters: Filters? = null,
) {
  companion object {
    fun create(
      sortKey: SortKey,
      connectionName: String?,
      sourceName: String?,
      sourceDefinitionName: String?,
      destinationName: String?,
      destinationDefinitionName: String?,
      lastSync: Long?,
      cursorId: UUID?,
      ascending: Boolean = true,
      filters: Filters? = null,
    ): Cursor =
      if (isValid(sortKey, connectionName, sourceName, sourceDefinitionName, destinationName, destinationDefinitionName, lastSync, cursorId)) {
        Cursor(
          sortKey,
          connectionName,
          sourceName,
          sourceDefinitionName,
          destinationName,
          destinationDefinitionName,
          lastSync,
          cursorId,
          ascending,
          filters,
        )
      } else {
        throw IllegalArgumentException(
          "Invalid cursor. sortKey=$sortKey connectionName=$connectionName sourceName=$sourceName " +
            "destinationName=$destinationName lastSync=$lastSync cursorId=$cursorId ascending=$ascending",
        )
      }

    fun isValid(
      sortKey: SortKey?,
      connectionName: String?,
      sourceName: String?,
      sourceDefinitionName: String?,
      destinationName: String?,
      destinationDefinitionName: String?,
      lastSync: Long?,
      cursorId: UUID?,
    ): Boolean {
      // If no sort keys provided, default to connection name sorting
      val actualSortKey = sortKey ?: SortKey.CONNECTION_NAME

      // For first page (all cursor values null), always valid
      val allCursorValuesNull =
        connectionName == null &&
          sourceName == null &&
          destinationName == null &&
          lastSync == null &&
          cursorId == null
      if (allCursorValuesNull) return true

      // For subsequent pages, must have cursorId and at least one sort field value
      if (cursorId == null) return false

      // Check that we have cursor values for the sort keys being used
      return when (actualSortKey) {
        SortKey.CONNECTION_NAME -> connectionName != null
        SortKey.SOURCE_NAME -> sourceName != null
        SortKey.SOURCE_DEFINITION_NAME -> sourceDefinitionName != null
        SortKey.DESTINATION_NAME -> destinationName != null
        SortKey.DESTINATION_DEFINITION_NAME -> destinationDefinitionName != null
        // Allow null lastSync values since connections may have never synced
        SortKey.LAST_SYNC -> true
      }
    }
  }
}

data class WorkspaceResourceCursorPagination(
  var cursor: Cursor?,
  var pageSize: Int,
) {
  companion object {
    @JvmStatic
    fun fromValues(
      sortKey: SortKey,
      connectionName: String?,
      sourceName: String?,
      sourceDefinitionName: String?,
      destinationName: String?,
      destinationDefinitionName: String?,
      lastSync: Long?,
      cursorId: UUID?,
      pageSize: Int?,
      ascending: Boolean?,
      filters: Filters?,
    ): WorkspaceResourceCursorPagination {
      val cursor =
        Cursor.create(
          sortKey = sortKey,
          connectionName = connectionName,
          sourceName = sourceName,
          sourceDefinitionName = sourceDefinitionName,
          destinationName = destinationName,
          destinationDefinitionName = destinationDefinitionName,
          lastSync = lastSync,
          cursorId = cursorId,
          ascending = ascending ?: true,
          filters = filters,
        )
      return WorkspaceResourceCursorPagination(cursor, pageSize ?: DEFAULT_PAGE_SIZE)
    }
  }
}

/**
 * Helper record to hold parsed sort key information.
 */
@VisibleForTesting
@JvmRecord
data class SortKeyInfo(
  val sortKey: SortKey,
  val ascending: Boolean,
)

/**
 * Parses the string-based sort key to extract field and direction.
 */
fun parseSortKey(sortKey: WebBackendConnectionListSortKey?): SortKeyInfo {
  if (sortKey == null) {
    return SortKeyInfo(SortKey.CONNECTION_NAME, true)
  }

  return when (sortKey) {
    WebBackendConnectionListSortKey.CONNECTION_NAME_ASC -> SortKeyInfo(SortKey.CONNECTION_NAME, true)
    WebBackendConnectionListSortKey.CONNECTION_NAME_DESC -> SortKeyInfo(SortKey.CONNECTION_NAME, false)
    WebBackendConnectionListSortKey.SOURCE_NAME_ASC -> SortKeyInfo(SortKey.SOURCE_NAME, true)
    WebBackendConnectionListSortKey.SOURCE_NAME_DESC -> SortKeyInfo(SortKey.SOURCE_NAME, false)
    WebBackendConnectionListSortKey.DESTINATION_NAME_ASC -> SortKeyInfo(SortKey.DESTINATION_NAME, true)
    WebBackendConnectionListSortKey.DESTINATION_NAME_DESC -> SortKeyInfo(SortKey.DESTINATION_NAME, false)
    WebBackendConnectionListSortKey.LAST_SYNC_ASC -> SortKeyInfo(SortKey.LAST_SYNC, true)
    WebBackendConnectionListSortKey.LAST_SYNC_DESC -> SortKeyInfo(SortKey.LAST_SYNC, false)
  }
}

/**
 * Parses the string-based sort key to extract field and direction.
 */
fun parseSortKey(
  sortKey: ActorListSortKey?,
  actorType: ActorType,
): SortKeyInfo {
  if (sortKey == null) {
    return SortKeyInfo(if (actorType == ActorType.SOURCE) SortKey.SOURCE_NAME else SortKey.DESTINATION_NAME, true)
  }

  return when (sortKey) {
    ActorListSortKey.ACTOR_NAME_ASC -> SortKeyInfo(if (actorType == ActorType.SOURCE) SortKey.SOURCE_NAME else SortKey.DESTINATION_NAME, true)
    ActorListSortKey.ACTOR_NAME_DESC -> SortKeyInfo(if (actorType == ActorType.SOURCE) SortKey.SOURCE_NAME else SortKey.DESTINATION_NAME, false)
    ActorListSortKey.ACTOR_DEFINITION_NAME_ASC ->
      SortKeyInfo(
        if (actorType ==
          ActorType.SOURCE
        ) {
          SortKey.SOURCE_DEFINITION_NAME
        } else {
          SortKey.DESTINATION_DEFINITION_NAME
        },
        true,
      )
    ActorListSortKey.ACTOR_DEFINITION_NAME_DESC ->
      SortKeyInfo(
        if (actorType ==
          ActorType.SOURCE
        ) {
          SortKey.SOURCE_DEFINITION_NAME
        } else {
          SortKey.DESTINATION_DEFINITION_NAME
        },
        false,
      )
    ActorListSortKey.LAST_SYNC_ASC -> SortKeyInfo(SortKey.LAST_SYNC, true)
    ActorListSortKey.LAST_SYNC_DESC -> SortKeyInfo(SortKey.LAST_SYNC, false)
  }
}

/**
 * Converts WebBackendConnectionListFilters to Filters for internal use.
 */
@VisibleForTesting
fun buildFilters(filters: WebBackendConnectionListFilters?): Filters? {
  if (filters == null) {
    return null
  }

  return Filters(
    filters.searchTerm,
    filters.sourceDefinitionIds,
    filters.destinationDefinitionIds, // Convert API enum values to config object
    if (filters.statuses == null) {
      null
    } else {
      filters.statuses
        .stream()
        .map { apiStatus: WebBackendConnectionListFilters.StatusesEnum -> ConnectionJobStatus.valueOf(apiStatus.name) }
        .collect(Collectors.toList())
    },
    if (filters.states == null) {
      null
    } else {
      filters.states
        .stream()
        .map { apiState: ActorStatus -> ActorStatus.valueOf(apiState.name) }
        .collect(Collectors.toList())
    },
    filters.tagIds,
  )
}

/**
 * Converts ActorListFilters to Filters for internal use.
 */
@VisibleForTesting
fun buildFilters(filters: ActorListFilters?): Filters? {
  if (filters == null) {
    return null
  }

  return Filters(
    filters.searchTerm,
    null,
    null,
    null,
    if (filters.states == null) {
      null
    } else {
      filters.states
        .stream()
        .map { apiState: ActorStatus -> ActorStatus.valueOf(apiState.name) }
        .collect(Collectors.toList())
    },
    null,
  )
}
