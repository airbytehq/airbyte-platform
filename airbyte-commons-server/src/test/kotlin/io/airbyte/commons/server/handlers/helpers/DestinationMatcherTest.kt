/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import io.airbyte.api.server.generated.models.DestinationRead
import io.airbyte.api.server.generated.models.DestinationSearch
import io.airbyte.commons.json.Jsons
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Test
import java.util.UUID

// MatchSearchHandler treats a destination as found when match() returns a read equal to the original
internal class DestinationMatcherTest {
  private val destinationRead =
    DestinationRead(
      destinationDefinitionId = UUID.randomUUID(),
      destinationId = UUID.randomUUID(),
      workspaceId = UUID.randomUUID(),
      connectionConfiguration = Jsons.jsonNode(mapOf("host" to "localhost", "port" to 5432)),
      name = "my destination",
      destinationName = "Postgres",
      createdAt = 1_700_000_000L,
    )

  @Test
  fun `search with matching fields returns the original read`() {
    val search = DestinationSearch(name = destinationRead.name, workspaceId = destinationRead.workspaceId)

    assertEquals(destinationRead, DestinationMatcher(search).match(destinationRead))
  }

  @Test
  fun `search with part of the configuration returns the original read`() {
    val search = DestinationSearch(connectionConfiguration = Jsons.jsonNode(mapOf("host" to "localhost")))

    assertEquals(destinationRead, DestinationMatcher(search).match(destinationRead))
  }

  @Test
  fun `search with a different name does not match`() {
    val search = DestinationSearch(name = "other destination")

    assertNotEquals(destinationRead, DestinationMatcher(search).match(destinationRead))
  }

  @Test
  fun `no search returns the original read`() {
    assertEquals(destinationRead, DestinationMatcher(null).match(destinationRead))
  }
}
