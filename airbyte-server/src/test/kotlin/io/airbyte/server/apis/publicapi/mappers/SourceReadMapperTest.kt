/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.server.generated.models.ResourceRequirements
import io.airbyte.api.server.generated.models.ScopedResourceRequirements
import io.airbyte.api.server.generated.models.SourceRead
import io.airbyte.commons.json.Jsons
import io.airbyte.publicApi.server.generated.models.JobTypeResourceLimit
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class SourceReadMapperTest {
  @Test
  fun `from should convert a SourceRead object from the config api to a SourceResponse`() {
    // Given
    val sourceRead =
      SourceRead(
        sourceId = UUID.randomUUID(),
        name = "sourceName",
        sourceDefinitionId = UUID.randomUUID(),
        workspaceId = UUID.randomUUID(),
        connectionConfiguration = Jsons.deserialize("{}"),
        sourceName = "source-definition",
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
