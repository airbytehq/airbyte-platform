/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.DestinationRead
import io.airbyte.commons.json.Jsons
import io.airbyte.publicApi.server.generated.models.JobTypeResourceLimit
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class DestinationReadMapperTest {
  @Test
  fun `from should convert a DestinationRead object from the config api to a DestinationResponse`() {
    // Given
    val destinationRead =
      DestinationRead().apply {
        destinationId = UUID.randomUUID()
        name = "destinationName"
        destinationDefinitionId = UUID.randomUUID()
        workspaceId = UUID.randomUUID()
        connectionConfiguration = Jsons.deserialize("{}")
        createdAt = 1L
        resourceAllocation =
          mockk {
            every { default } returns
              mockk {
                every { cpuRequest } returns "1"
                every { cpuLimit } returns "2"
                every { memoryRequest } returns "3"
                every { memoryLimit } returns "4"
                every { ephemeralStorageRequest } returns "5"
                every { ephemeralStorageLimit } returns "6"
              }
            every { jobSpecific } returns emptyList()
          }
      }

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
