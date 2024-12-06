package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.DestinationRead
import io.airbyte.commons.json.Jsons
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class DestinationReadMapperTest {
  @Test
  fun `from should convert a DestinationRead object from the config api to a DestinationResponse`() {
    // Given
    val destinationRead = DestinationRead()
    destinationRead.destinationId = UUID.randomUUID()
    destinationRead.name = "destinationName"
    destinationRead.destinationDefinitionId = UUID.randomUUID()
    destinationRead.workspaceId = UUID.randomUUID()
    destinationRead.connectionConfiguration = Jsons.deserialize("{}")
    destinationRead.createdAt = 1L

    // When
    val destinationResponse = DestinationReadMapper.from(destinationRead)

    // Then
    assertEquals(destinationRead.destinationId.toString(), destinationResponse.destinationId)
    assertEquals(destinationRead.name, destinationResponse.name)
    assertEquals(DEFINITION_ID_TO_DESTINATION_NAME.getOrDefault(destinationRead.destinationDefinitionId, ""), destinationResponse.destinationType)
    assertEquals(destinationRead.workspaceId.toString(), destinationResponse.workspaceId)
    assertEquals(destinationRead.connectionConfiguration, destinationResponse.configuration)
    assertEquals(destinationRead.destinationDefinitionId.toString(), destinationResponse.definitionId)
    assertEquals(destinationRead.createdAt, destinationResponse.createdAt)
  }
}
