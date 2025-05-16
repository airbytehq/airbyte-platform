/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.ConnectionRead
import io.airbyte.api.model.generated.ConnectionScheduleType
import io.airbyte.api.model.generated.ConnectionStatus
import io.airbyte.api.model.generated.NamespaceDefinitionType
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
    connectionRead.dataplaneGroupId = UUID.randomUUID()
    connectionRead.scheduleType = ConnectionScheduleType.MANUAL
    connectionRead.sourceId = UUID.randomUUID()
    connectionRead.destinationId = UUID.randomUUID()
    connectionRead.namespaceDefinition = NamespaceDefinitionType.DESTINATION
    connectionRead.namespaceFormat = "namespaceFormat"
    connectionRead.prefix = "prefix"
    connectionRead.createdAt = 1L

    val workspaceId = UUID.randomUUID()
    val result = ConnectionReadMapper.from(connectionRead, workspaceId, "US")

    assertEquals(connectionRead.connectionId.toString(), result.connectionId)
    assertEquals(connectionRead.name, result.name)
    assertEquals(connectionRead.status.toString(), connectionRead.status.toString())
    assertEquals("us", result.dataResidency)
    assertEquals(ScheduleTypeWithBasicEnum.MANUAL, result.schedule.scheduleType)
    assertEquals(connectionRead.sourceId.toString(), result.sourceId)
    assertEquals(connectionRead.destinationId.toString(), result.destinationId)
    assertEquals(connectionRead.createdAt, result.createdAt)
  }

  @Test
  internal fun `test mapping for OSS edition with null dataplaneGroupId and inputDataResidency`() {
    val connectionRead = ConnectionRead()
    connectionRead.connectionId = UUID.randomUUID()
    connectionRead.name = "ossConnection"
    connectionRead.status = ConnectionStatus.INACTIVE
    connectionRead.dataplaneGroupId = null
    connectionRead.scheduleType = ConnectionScheduleType.BASIC
    connectionRead.sourceId = UUID.randomUUID()
    connectionRead.destinationId = UUID.randomUUID()
    connectionRead.namespaceDefinition = NamespaceDefinitionType.SOURCE
    connectionRead.namespaceFormat = "custom"
    connectionRead.prefix = ""
    connectionRead.createdAt = 100L

    val workspaceId = UUID.randomUUID()
    val result = ConnectionReadMapper.from(connectionRead, workspaceId, "AUTO")

    assertEquals("auto", result.dataResidency)
  }

  @Test
  internal fun `test mapping for OSS edition with inputDataResidency override`() {
    val connectionRead = ConnectionRead()
    connectionRead.connectionId = UUID.randomUUID()
    connectionRead.name = "customDRConnection"
    connectionRead.status = ConnectionStatus.ACTIVE
    connectionRead.dataplaneGroupId = null
    connectionRead.scheduleType = ConnectionScheduleType.MANUAL
    connectionRead.sourceId = UUID.randomUUID()
    connectionRead.destinationId = UUID.randomUUID()
    connectionRead.namespaceDefinition = NamespaceDefinitionType.CUSTOMFORMAT
    connectionRead.namespaceFormat = "custom-ns"
    connectionRead.prefix = "pfx_"
    connectionRead.createdAt = 999L

    val workspaceId = UUID.randomUUID()
    val result = ConnectionReadMapper.from(connectionRead, workspaceId, "AUTO")

    assertEquals("auto", result.dataResidency)
  }
}
