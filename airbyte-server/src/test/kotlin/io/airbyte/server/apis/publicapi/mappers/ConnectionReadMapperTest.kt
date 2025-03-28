/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.ConnectionRead
import io.airbyte.api.model.generated.ConnectionScheduleType
import io.airbyte.api.model.generated.ConnectionStatus
import io.airbyte.api.model.generated.NamespaceDefinitionType
import io.airbyte.commons.constants.GEOGRAPHY_US
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

internal class ConnectionReadMapperTest {
  @Test
  internal fun testConnectionReadMapper() {
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
    val connectionResponse = ConnectionReadMapper.from(connectionRead, workspaceId)

    assertEquals(connectionResponse.connectionId, connectionRead.connectionId.toString())
    assertEquals(connectionResponse.name, connectionRead.name)
    assertEquals(connectionResponse.status.toString(), connectionRead.status.toString())
    assertEquals(connectionResponse.dataResidency, connectionRead.geography.lowercase())
    assertEquals(connectionResponse.schedule.scheduleType.toString(), connectionRead.scheduleType.toString())
    assertEquals(connectionResponse.sourceId, connectionRead.sourceId.toString())
    assertEquals(connectionResponse.destinationId, connectionRead.destinationId.toString())
    assertEquals(connectionResponse.createdAt, connectionRead.createdAt)
  }
}
