/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.ConnectionRead
import io.airbyte.api.model.generated.ConnectionScheduleType
import io.airbyte.api.model.generated.ConnectionStatus
import io.airbyte.api.model.generated.NamespaceDefinitionType
import io.airbyte.commons.constants.GEOGRAPHY_AUTO
import io.airbyte.commons.constants.GEOGRAPHY_US
import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.publicApi.server.generated.models.ScheduleTypeWithBasicEnum
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

internal class ConnectionReadMapperTest {
  @Test
  internal fun `test mapping for Airbyte Cloud`() {
    val connectionRead = ConnectionRead()
    connectionRead.connectionId = UUID.randomUUID()
    connectionRead.name = "testconnection"
    connectionRead.status = ConnectionStatus.ACTIVE
    connectionRead.geography = GEOGRAPHY_US
    connectionRead.scheduleType = ConnectionScheduleType.MANUAL
    connectionRead.sourceId = UUID.randomUUID()
    connectionRead.destinationId = UUID.randomUUID()
    connectionRead.namespaceDefinition = NamespaceDefinitionType.DESTINATION
    connectionRead.namespaceFormat = "namespaceFormat"
    connectionRead.prefix = "prefix"
    connectionRead.createdAt = 1L

    val workspaceId = UUID.randomUUID()
    val result = ConnectionReadMapper.from(connectionRead, workspaceId, AirbyteEdition.CLOUD)

    assertEquals(connectionRead.connectionId.toString(), result.connectionId)
    assertEquals(connectionRead.name, result.name)
    assertEquals(connectionRead.status.toString(), connectionRead.status.toString())
    assertEquals(GEOGRAPHY_US.lowercase(), result.dataResidency)
    assertEquals(ScheduleTypeWithBasicEnum.MANUAL, result.schedule.scheduleType)
    assertEquals(connectionRead.sourceId.toString(), result.sourceId)
    assertEquals(connectionRead.destinationId.toString(), result.destinationId)
    assertEquals(connectionRead.createdAt, result.createdAt)
  }

  @Test
  internal fun `test mapping for OSS edition with null geography and inputDataResidency`() {
    val connectionRead = ConnectionRead()
    connectionRead.connectionId = UUID.randomUUID()
    connectionRead.name = "ossConnection"
    connectionRead.status = ConnectionStatus.INACTIVE
    connectionRead.geography = null
    connectionRead.scheduleType = ConnectionScheduleType.BASIC
    connectionRead.sourceId = UUID.randomUUID()
    connectionRead.destinationId = UUID.randomUUID()
    connectionRead.namespaceDefinition = NamespaceDefinitionType.SOURCE
    connectionRead.namespaceFormat = "custom"
    connectionRead.prefix = ""
    connectionRead.createdAt = 100L

    val workspaceId = UUID.randomUUID()
    val result = ConnectionReadMapper.from(connectionRead, workspaceId, AirbyteEdition.COMMUNITY)

    assertEquals(GEOGRAPHY_AUTO.lowercase(), result.dataResidency)
  }

  @Test
  internal fun `test mapping for OSS edition with inputDataResidency override`() {
    val connectionRead = ConnectionRead()
    connectionRead.connectionId = UUID.randomUUID()
    connectionRead.name = "customDRConnection"
    connectionRead.status = ConnectionStatus.ACTIVE
    connectionRead.geography = null
    connectionRead.scheduleType = ConnectionScheduleType.MANUAL
    connectionRead.sourceId = UUID.randomUUID()
    connectionRead.destinationId = UUID.randomUUID()
    connectionRead.namespaceDefinition = NamespaceDefinitionType.CUSTOMFORMAT
    connectionRead.namespaceFormat = "custom-ns"
    connectionRead.prefix = "pfx_"
    connectionRead.createdAt = 999L

    val workspaceId = UUID.randomUUID()
    val result = ConnectionReadMapper.from(connectionRead, workspaceId, AirbyteEdition.ENTERPRISE)

    assertEquals("auto", result.dataResidency)
  }
}
