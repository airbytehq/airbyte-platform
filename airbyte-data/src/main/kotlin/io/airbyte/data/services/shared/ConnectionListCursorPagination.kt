/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.shared

import io.airbyte.api.model.generated.ActorStatus
import java.util.UUID

const val DEFAULT_PAGE_SIZE = 20

enum class ConnectionSortKey {
  CONNECTION_NAME,
  SOURCE_NAME,
  DESTINATION_NAME,
  LAST_SYNC,
}

enum class ConnectionJobStatus {
  HEALTHY,
  FAILED,
  RUNNING,
}

/**
 * Filter parameters for connection queries.
 */
data class ConnectionFilters(
  val searchTerm: String? = null,
  val sourceDefinitionIds: List<UUID>? = null,
  val destinationDefinitionIds: List<UUID>? = null,
  val statuses: List<ConnectionJobStatus>? = null,
  val states: List<ActorStatus>? = null,
  val tagIds: List<UUID>? = null,
)

data class ConnectionListCursor(
  var sortKey: ConnectionSortKey,
  var connectionName: String?,
  var sourceName: String?,
  var destinationName: String?,
  var lastSync: Long?,
  var connectionId: UUID?,
  var ascending: Boolean = true,
  var filters: ConnectionFilters? = null,
) {
  companion object {
    fun create(
      sortKey: ConnectionSortKey?,
      connectionName: String?,
      sourceName: String?,
      destinationName: String?,
      lastSync: Long?,
      connectionId: UUID?,
      ascending: Boolean = true,
      filters: ConnectionFilters? = null,
    ): ConnectionListCursor =
      if (isValid(sortKey, connectionName, sourceName, destinationName, lastSync, connectionId)) {
        ConnectionListCursor(
          sortKey ?: ConnectionSortKey.CONNECTION_NAME,
          connectionName,
          sourceName,
          destinationName,
          lastSync,
          connectionId,
          ascending,
          filters,
        )
      } else {
        throw IllegalArgumentException(
          "Invalid cursor. sortKey=$sortKey connectionName=$connectionName sourceName=$sourceName " +
            "destinationName=$destinationName lastSync=$lastSync connectionId=$connectionId ascending=$ascending",
        )
      }

    fun isValid(
      sortKey: ConnectionSortKey?,
      connectionName: String?,
      sourceName: String?,
      destinationName: String?,
      lastSync: Long?,
      connectionId: UUID?,
    ): Boolean {
      // If no sort keys provided, default to connection name sorting
      val actualSortKey = sortKey ?: ConnectionSortKey.CONNECTION_NAME

      // For first page (all cursor values null), always valid
      val allCursorValuesNull =
        connectionName == null &&
          sourceName == null &&
          destinationName == null &&
          lastSync == null &&
          connectionId == null
      if (allCursorValuesNull) return true

      // For subsequent pages, must have connectionId and at least one sort field value
      if (connectionId == null) return false

      // Check that we have cursor values for the sort keys being used
      return when (actualSortKey) {
        ConnectionSortKey.CONNECTION_NAME -> connectionName != null
        ConnectionSortKey.SOURCE_NAME -> sourceName != null
        ConnectionSortKey.DESTINATION_NAME -> destinationName != null
        // Allow null lastSync values since connections may have never synced
        ConnectionSortKey.LAST_SYNC -> true
      }
    }
  }
}

data class ConnectionListCursorPagination(
  var cursor: ConnectionListCursor?,
  var pageSize: Int,
) {
  companion object {
    @JvmStatic
    fun fromValues(
      sortKey: ConnectionSortKey?,
      connectionName: String?,
      sourceName: String?,
      destinationName: String?,
      lastSync: Long?,
      connectionId: UUID?,
      pageSize: Int?,
      ascending: Boolean?,
      filters: ConnectionFilters?,
    ): ConnectionListCursorPagination {
      val cursor =
        ConnectionListCursor.create(
          sortKey = sortKey,
          connectionName = connectionName,
          sourceName = sourceName,
          destinationName = destinationName,
          lastSync = lastSync,
          connectionId = connectionId,
          ascending = ascending ?: true,
          filters = filters,
        )
      return ConnectionListCursorPagination(cursor, pageSize ?: DEFAULT_PAGE_SIZE)
    }
  }
}
