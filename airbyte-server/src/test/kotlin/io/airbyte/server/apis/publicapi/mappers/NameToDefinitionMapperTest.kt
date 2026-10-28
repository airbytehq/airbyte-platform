/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class NameToDefinitionMapperTest {
  @Test
  fun `hubspot destination mapping should be present in both directions`() {
    val hubspotDestinationDefinitionId = UUID.fromString("c8ccd253-8525-4bbd-801c-f0b84ac71f61")

    assertEquals(hubspotDestinationDefinitionId, DESTINATION_NAME_TO_DEFINITION_ID["hubspot"])
    assertEquals("hubspot", DEFINITION_ID_TO_DESTINATION_NAME[hubspotDestinationDefinitionId])
  }
}
