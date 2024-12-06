package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.ConnectionRead
import io.airbyte.api.model.generated.ConnectionScheduleType
import io.airbyte.api.model.generated.ConnectionStatus
import io.airbyte.api.model.generated.Geography
import io.airbyte.api.model.generated.NamespaceDefinitionType
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
    connectionRead.geography = Geography.US
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
    assertEquals(connectionResponse.dataResidency.toString(), connectionRead.geography.toString())
    assertEquals(connectionResponse.schedule.scheduleType.toString(), connectionRead.scheduleType.toString())
    assertEquals(connectionResponse.sourceId, connectionRead.sourceId.toString())
    assertEquals(connectionResponse.destinationId, connectionRead.destinationId.toString())
    assertEquals(connectionResponse.createdAt, connectionRead.createdAt)
  }
}
