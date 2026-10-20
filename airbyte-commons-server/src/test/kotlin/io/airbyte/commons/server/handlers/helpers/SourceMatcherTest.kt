/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import io.airbyte.api.server.generated.models.SourceRead
import io.airbyte.api.server.generated.models.SourceSearch
import io.airbyte.commons.json.Jsons
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Test
import java.util.UUID

// MatchSearchHandler treats a source as found when match() returns a read equal to the original
internal class SourceMatcherTest {
  private val sourceRead =
    SourceRead(
      sourceDefinitionId = UUID.randomUUID(),
      sourceId = UUID.randomUUID(),
      workspaceId = UUID.randomUUID(),
      connectionConfiguration = Jsons.jsonNode(mapOf("host" to "localhost", "port" to 5432)),
      name = "my source",
      sourceName = "Postgres",
      createdAt = 1_700_000_000L,
    )

  @Test
  fun `search with matching fields returns the original read`() {
    val search = SourceSearch(name = sourceRead.name, workspaceId = sourceRead.workspaceId)

    assertEquals(sourceRead, SourceMatcher(search).match(sourceRead))
  }

  @Test
  fun `search with part of the configuration returns the original read`() {
    val search = SourceSearch(connectionConfiguration = Jsons.jsonNode(mapOf("host" to "localhost")))

    assertEquals(sourceRead, SourceMatcher(search).match(sourceRead))
  }

  @Test
  fun `search with a different name does not match`() {
    val search = SourceSearch(name = "other source")

    assertNotEquals(sourceRead, SourceMatcher(search).match(sourceRead))
  }

  @Test
  fun `no search returns the original read`() {
    assertEquals(sourceRead, SourceMatcher(null).match(sourceRead))
  }
}
