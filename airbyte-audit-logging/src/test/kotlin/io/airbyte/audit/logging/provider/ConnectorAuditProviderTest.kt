/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.audit.logging.provider

import io.airbyte.api.model.generated.DestinationCreate
import io.airbyte.api.model.generated.DestinationRead
import io.airbyte.api.model.generated.SourceCreate
import io.airbyte.api.model.generated.SourceIdRequestBody
import io.airbyte.api.model.generated.SourceRead
import io.airbyte.api.model.generated.SourceUpdate
import io.airbyte.commons.json.Jsons
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Test
import java.util.UUID

class ConnectorAuditProviderTest {
  private val provider = ConnectorAuditProvider()

  @Test
  fun `createSource request summary omits the connectionConfiguration but keeps identity`() {
    val workspaceId = UUID.randomUUID()
    val definitionId = UUID.randomUUID()
    val request =
      SourceCreate()
        .name("my postgres")
        .sourceDefinitionId(definitionId)
        .workspaceId(workspaceId)
        .connectionConfiguration(Jsons.jsonNode(mapOf("password" to "super-secret-value")))
        .secretId("secret-id-value")

    val summary = provider.generateSummaryFromRequest(request)

    assertFalse(summary.contains("super-secret-value"), "credential value leaked into audit summary: $summary")
    assertFalse(summary.contains("connectionConfiguration"), "connectionConfiguration key leaked into audit summary: $summary")
    assertFalse(summary.contains("secret-id-value"), "secretId value leaked into audit summary: $summary")

    val node = Jsons.deserialize(summary)
    assertEquals("my postgres", node.get("name").asText())
    assertEquals(definitionId.toString(), node.get("sourceDefinitionId").asText())
    assertEquals(workspaceId.toString(), node.get("workspaceId").asText())
  }

  @Test
  fun `createSource response summary carries the newly assigned sourceId`() {
    val sourceId = UUID.randomUUID()
    val result =
      SourceRead()
        .sourceId(sourceId)
        .name("my postgres")
        .sourceName("Postgres")
        .workspaceId(UUID.randomUUID())
        .sourceDefinitionId(UUID.randomUUID())
        .connectionConfiguration(Jsons.jsonNode(mapOf("password" to "super-secret-value")))

    val summary = provider.generateSummaryFromResult(result)

    // The response body carries connectionConfiguration too, so it is allowlisted the same way.
    assertFalse(summary.contains("super-secret-value"), "credential value leaked into audit summary: $summary")

    val node = Jsons.deserialize(summary)
    assertEquals(sourceId.toString(), node.get("sourceId").asText())
    assertEquals("Postgres", node.get("sourceName").asText())
  }

  @Test
  fun `updateSource request summary identifies which source was updated`() {
    val sourceId = UUID.randomUUID()
    val request =
      SourceUpdate()
        .sourceId(sourceId)
        .name("renamed")
        .connectionConfiguration(Jsons.jsonNode(mapOf("api_key" to "super-secret-value")))

    val summary = provider.generateSummaryFromRequest(request)

    assertFalse(summary.contains("super-secret-value"), "credential value leaked into audit summary: $summary")
    val node = Jsons.deserialize(summary)
    assertEquals(sourceId.toString(), node.get("sourceId").asText())
    assertEquals("renamed", node.get("name").asText())
  }

  @Test
  fun `deleteSource request summary identifies which source was deleted`() {
    val sourceId = UUID.randomUUID()

    val node = Jsons.deserialize(provider.generateSummaryFromRequest(SourceIdRequestBody().sourceId(sourceId)))

    assertEquals(sourceId.toString(), node.get("sourceId").asText())
  }

  @Test
  fun `destination bodies are allowlisted the same way as source bodies`() {
    val workspaceId = UUID.randomUUID()
    val request =
      DestinationCreate()
        .name("my snowflake")
        .destinationDefinitionId(UUID.randomUUID())
        .workspaceId(workspaceId)
        .connectionConfiguration(Jsons.jsonNode(mapOf("password" to "super-secret-value")))

    val summary = provider.generateSummaryFromRequest(request)

    assertFalse(summary.contains("super-secret-value"), "credential value leaked into audit summary: $summary")
    val node = Jsons.deserialize(summary)
    assertEquals("my snowflake", node.get("name").asText())
    assertEquals(workspaceId.toString(), node.get("workspaceId").asText())
  }

  @Test
  fun `destination response summary carries the newly assigned destinationId`() {
    val destinationId = UUID.randomUUID()
    val result = DestinationRead().destinationId(destinationId).name("my snowflake").destinationName("Snowflake")

    val node = Jsons.deserialize(provider.generateSummaryFromResult(result))

    assertEquals(destinationId.toString(), node.get("destinationId").asText())
    assertEquals("Snowflake", node.get("destinationName").asText())
  }

  @Test
  fun `null request and result yield an empty summary`() {
    assertEquals(AuditProvider.EMPTY_SUMMARY, provider.generateSummaryFromRequest(null))
    assertEquals(AuditProvider.EMPTY_SUMMARY, provider.generateSummaryFromResult(null))
  }
}
