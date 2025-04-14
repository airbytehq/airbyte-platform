/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.SourceRead
import io.airbyte.commons.json.Jsons
import io.airbyte.publicApi.server.generated.models.JobTypeResourceLimit
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class SourceReadMapperTest {
  @Test
  fun `from should convert a SourceRead object from the config api to a SourceResponse`() {
    // Given
    val sourceRead =
      SourceRead().apply {
        sourceId = UUID.randomUUID()
        name = "sourceName"
        sourceDefinitionId = UUID.randomUUID()
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
    val sourceResponse = SourceReadMapper.from(sourceRead)

    // Then
    assertEquals(sourceRead.sourceId.toString(), sourceResponse.sourceId)
    assertEquals(sourceRead.name, sourceResponse.name)
    assertEquals(DEFINITION_ID_TO_SOURCE_NAME.getOrDefault(sourceRead.sourceDefinitionId, ""), sourceResponse.sourceType)
    assertEquals(sourceRead.workspaceId.toString(), sourceResponse.workspaceId)
    assertEquals(sourceRead.connectionConfiguration, sourceResponse.configuration)
    assertEquals(sourceRead.sourceDefinitionId.toString(), sourceResponse.definitionId)
    assertEquals(sourceRead.createdAt, sourceResponse.createdAt)

    assertEquals(emptyList<JobTypeResourceLimit>(), sourceResponse.resourceAllocation?.jobSpecific)
    assertEquals("1", sourceResponse.resourceAllocation?.default?.cpuRequest)
    assertEquals("2", sourceResponse.resourceAllocation?.default?.cpuLimit)
    assertEquals("3", sourceResponse.resourceAllocation?.default?.memoryRequest)
    assertEquals("4", sourceResponse.resourceAllocation?.default?.memoryLimit)
    assertEquals("5", sourceResponse.resourceAllocation?.default?.ephemeralStorageRequest)
    assertEquals("6", sourceResponse.resourceAllocation?.default?.ephemeralStorageLimit)
  }
}
