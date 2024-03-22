package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.ConnectionRead
import io.airbyte.api.model.generated.ConnectionScheduleType
import io.airbyte.api.model.generated.ConnectionStatus
import io.airbyte.api.model.generated.Geography
import io.airbyte.api.model.generated.NamespaceDefinitionType
import org.junit.jupiter.api.Test
import java.util.UUID

class ConnectionReadMapperTest {
  @Test
  fun testConnectionReadMapper() {
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

    val workspaceId = UUID.randomUUID()
    val connectionResponse = ConnectionReadMapper.from(connectionRead, workspaceId)

    assert(connectionResponse.connectionId == connectionRead.connectionId)
    assert(connectionResponse.name == connectionRead.name)
    assert(connectionResponse.status.toString() == connectionRead.status.toString())
    assert(connectionResponse.dataResidency.toString() == connectionRead.geography.toString())
    assert(connectionResponse.schedule.scheduleType.toString() == connectionRead.scheduleType.toString())
    assert(connectionResponse.sourceId == connectionRead.sourceId)
    assert(connectionResponse.destinationId == connectionRead.destinationId)
  }
}
