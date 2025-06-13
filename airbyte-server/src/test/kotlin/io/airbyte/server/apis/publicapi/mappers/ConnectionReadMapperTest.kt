/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.ConnectionRead
import io.airbyte.api.model.generated.ConnectionScheduleType
import io.airbyte.api.model.generated.ConnectionStatus
import io.airbyte.api.model.generated.DestinationSyncMode
import io.airbyte.api.model.generated.NamespaceDefinitionType
import io.airbyte.api.model.generated.SyncMode
import io.airbyte.publicApi.server.generated.models.ConnectionSyncModeEnum
import io.airbyte.publicApi.server.generated.models.ScheduleTypeWithBasicEnum
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.UUID
import java.util.stream.Stream

internal class ConnectionReadMapperTest {
  @Test
  internal fun `test mapping for Airbyte Cloud`() {
    val connectionRead = ConnectionRead()
    connectionRead.connectionId = UUID.randomUUID()
    connectionRead.name = "testconnection"
    connectionRead.status = ConnectionStatus.ACTIVE
    connectionRead.dataplaneGroupId = UUID.randomUUID()
    connectionRead.scheduleType = ConnectionScheduleType.MANUAL
    connectionRead.sourceId = UUID.randomUUID()
    connectionRead.destinationId = UUID.randomUUID()
    connectionRead.namespaceDefinition = NamespaceDefinitionType.DESTINATION
    connectionRead.namespaceFormat = "namespaceFormat"
    connectionRead.prefix = "prefix"
    connectionRead.createdAt = 1L

    val workspaceId = UUID.randomUUID()
    val result = ConnectionReadMapper.from(connectionRead, workspaceId)

    assertEquals(connectionRead.connectionId.toString(), result.connectionId)
    assertEquals(connectionRead.name, result.name)
    assertEquals(connectionRead.status.toString(), connectionRead.status.toString())
    assertEquals(ScheduleTypeWithBasicEnum.MANUAL, result.schedule.scheduleType)
    assertEquals(connectionRead.sourceId.toString(), result.sourceId)
    assertEquals(connectionRead.destinationId.toString(), result.destinationId)
    assertEquals(connectionRead.createdAt, result.createdAt)
  }

  @ParameterizedTest
  @MethodSource("validSyncModeCombinations")
  internal fun `test syncModesToConnectionSyncModeEnum - valid combinations`(
    sourceSyncMode: SyncMode,
    destinationSyncMode: DestinationSyncMode,
    expected: ConnectionSyncModeEnum,
  ) {
    val result = ConnectionReadMapper.syncModesToConnectionSyncModeEnum(sourceSyncMode, destinationSyncMode)
    assertEquals(expected, result)
  }

  companion object {
    @JvmStatic
    fun validSyncModeCombinations(): Stream<Arguments> =
      Stream.of(
        // FULL_REFRESH combinations
        Arguments.of(SyncMode.FULL_REFRESH, DestinationSyncMode.OVERWRITE, ConnectionSyncModeEnum.FULL_REFRESH_OVERWRITE),
        Arguments.of(SyncMode.FULL_REFRESH, DestinationSyncMode.APPEND, ConnectionSyncModeEnum.FULL_REFRESH_APPEND),
        Arguments.of(SyncMode.FULL_REFRESH, DestinationSyncMode.OVERWRITE_DEDUP, ConnectionSyncModeEnum.FULL_REFRESH_OVERWRITE_DEDUPED),
        Arguments.of(SyncMode.FULL_REFRESH, DestinationSyncMode.UPDATE, ConnectionSyncModeEnum.FULL_REFRESH_UPDATE),
        Arguments.of(SyncMode.FULL_REFRESH, DestinationSyncMode.SOFT_DELETE, ConnectionSyncModeEnum.FULL_REFRESH_SOFT_DELETE),
        // INCREMENTAL combinations
        Arguments.of(SyncMode.INCREMENTAL, DestinationSyncMode.APPEND, ConnectionSyncModeEnum.INCREMENTAL_APPEND),
        Arguments.of(SyncMode.INCREMENTAL, DestinationSyncMode.APPEND_DEDUP, ConnectionSyncModeEnum.INCREMENTAL_DEDUPED_HISTORY),
        Arguments.of(SyncMode.INCREMENTAL, DestinationSyncMode.UPDATE, ConnectionSyncModeEnum.INCREMENTAL_UPDATE),
        Arguments.of(SyncMode.INCREMENTAL, DestinationSyncMode.SOFT_DELETE, ConnectionSyncModeEnum.INCREMENTAL_SOFT_DELETE),
      )
  }
}
