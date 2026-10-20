/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.server.generated.models.DestinationRead
import io.airbyte.api.server.generated.models.ResourceRequirements
import io.airbyte.api.server.generated.models.ScopedResourceRequirements
import io.airbyte.commons.json.Jsons
import io.airbyte.publicApi.server.generated.models.JobTypeResourceLimit
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class DestinationReadMapperTest {
  @Test
  fun `from should convert a DestinationRead object from the config api to a DestinationResponse`() {
    // Given
    val destinationRead =
      DestinationRead(
        destinationId = UUID.randomUUID(),
        name = "destinationName",
        destinationDefinitionId = UUID.randomUUID(),
        workspaceId = UUID.randomUUID(),
        connectionConfiguration = Jsons.deserialize("{}"),
        destinationName = "destination-definition",
        createdAt = 1L,
        resourceAllocation =
          ScopedResourceRequirements(
            default =
              ResourceRequirements(
                cpuRequest = "1",
                cpuLimit = "2",
                memoryRequest = "3",
                memoryLimit = "4",
                ephemeralStorageRequest = "5",
                ephemeralStorageLimit = "6",
              ),
            jobSpecific = emptyList(),
          ),
      )

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

    assertEquals(emptyList<JobTypeResourceLimit>(), destinationResponse.resourceAllocation?.jobSpecific)
    assertEquals("1", destinationResponse.resourceAllocation?.default?.cpuRequest)
    assertEquals("2", destinationResponse.resourceAllocation?.default?.cpuLimit)
    assertEquals("3", destinationResponse.resourceAllocation?.default?.memoryRequest)
    assertEquals("4", destinationResponse.resourceAllocation?.default?.memoryLimit)
    assertEquals("5", destinationResponse.resourceAllocation?.default?.ephemeralStorageRequest)
    assertEquals("6", destinationResponse.resourceAllocation?.default?.ephemeralStorageLimit)
  }
}
