/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq

import io.airbyte.data.services.shared.SortKey
import io.airbyte.data.services.shared.WorkspaceResourceCursorPagination.Companion.fromValues
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.UUID

internal class WorkspaceResourceCursorPaginationTest {
  companion object {
    @JvmStatic
    fun cursorValuesProvider() =
      listOf(
        Arguments.of(null, null, 5),
        Arguments.of("connectionName", UUID.randomUUID(), 10),
      )
  }

  @ParameterizedTest
  @MethodSource("cursorValuesProvider")
  fun testCreateCursorFromValues(
    connectionName: String?,
    connectionId: UUID?,
    pageSize: Int,
  ) {
    val cursor =
      fromValues(
        sortKey = SortKey.CONNECTION_NAME,
        connectionName = connectionName,
        sourceName = null,
        sourceDefinitionName = null,
        destinationName = null,
        destinationDefinitionName = null,
        lastSync = null,
        cursorId = connectionId,
        pageSize = pageSize,
        ascending = true,
        filters = null,
      )

    Assertions.assertNotNull(cursor)
    Assertions.assertEquals(connectionName, cursor.cursor?.connectionName)
    Assertions.assertEquals(connectionId, cursor.cursor?.cursorId)
    Assertions.assertEquals(pageSize, cursor.pageSize)
  }

  @Test
  fun testCursorValidation() {
    Assertions.assertThrows(IllegalArgumentException::class.java) {
      fromValues(
        sortKey = SortKey.CONNECTION_NAME,
        connectionName = null,
        sourceName = null,
        sourceDefinitionName = null,
        destinationName = null,
        destinationDefinitionName = null,
        lastSync = null,
        cursorId = UUID.randomUUID(),
        pageSize = 5,
        ascending = true,
        filters = null,
      )
    }
  }

  @Test
  fun testLastSyncSortingAllowsNullValues() {
    // This should NOT throw an exception - null lastSync is valid for connections that have never synced
    val cursor =
      fromValues(
        sortKey = SortKey.LAST_SYNC,
        connectionName = null,
        sourceName = null,
        sourceDefinitionName = null,
        destinationName = null,
        destinationDefinitionName = null,
        lastSync = null,
        cursorId = UUID.randomUUID(),
        pageSize = 10,
        ascending = true,
        filters = null,
      )

    Assertions.assertNotNull(cursor)
    Assertions.assertNotNull(cursor.cursor)
    Assertions.assertEquals(SortKey.LAST_SYNC, cursor.cursor?.sortKey)
    Assertions.assertNull(cursor.cursor?.lastSync)
  }
}
