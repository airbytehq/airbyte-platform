/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.server.generated.models.AirbyteCatalog
import io.airbyte.api.server.generated.models.ConnectionRead
import io.airbyte.api.server.generated.models.ConnectionScheduleData
import io.airbyte.api.server.generated.models.ConnectionScheduleDataCron
import io.airbyte.api.server.generated.models.ConnectionScheduleType
import io.airbyte.api.server.generated.models.ConnectionStatus
import io.airbyte.api.server.generated.models.DestinationSyncMode
import io.airbyte.api.server.generated.models.NamespaceDefinitionType
import io.airbyte.api.server.generated.models.SyncMode
import io.airbyte.publicApi.server.generated.models.ConnectionSyncModeEnum
import io.airbyte.publicApi.server.generated.models.ScheduleTypeWithBasicEnum
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.UUID

internal class ConnectionReadMapperTest {
  @Test
  internal fun `test mapping for Airbyte Cloud`() {
    val connectionRead =
      ConnectionRead(
        connectionId = UUID.randomUUID(),
        name = "testconnection",
        status = ConnectionStatus.ACTIVE,
        dataplaneGroupId = UUID.randomUUID(),
        scheduleType = ConnectionScheduleType.MANUAL,
        sourceId = UUID.randomUUID(),
        destinationId = UUID.randomUUID(),
        namespaceDefinition = NamespaceDefinitionType.DESTINATION,
        namespaceFormat = "namespaceFormat",
        prefix = "prefix",
        createdAt = 1L,
        syncCatalog = AirbyteCatalog(streams = emptyList()),
        breakingChange = false,
      )

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

  @Test
  internal fun `maps cron timezone in the read response suffix`() {
    val connectionRead =
      ConnectionRead(
        connectionId = UUID.randomUUID(),
        name = "testconnection",
        status = ConnectionStatus.ACTIVE,
        scheduleType = ConnectionScheduleType.CRON,
        scheduleData =
          ConnectionScheduleData(
            cron = ConnectionScheduleDataCron(cronExpression = "0 0 0 0 0 0", cronTimeZone = "America/New_York"),
          ),
        sourceId = UUID.randomUUID(),
        destinationId = UUID.randomUUID(),
        createdAt = 1L,
        syncCatalog = AirbyteCatalog(streams = emptyList()),
        breakingChange = false,
      )

    val result = ConnectionReadMapper.from(connectionRead, UUID.randomUUID())

    assertEquals("0 0 0 0 0 0 America/New_York", result.schedule.cronExpression)
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
    fun validSyncModeCombinations() =
      listOf(
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
