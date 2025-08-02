/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.commons.server.support.AuthenticationFields.WORKSPACE_IDS_FIELD_NAME
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.Optional
import java.util.UUID

/**
 * Test suite for the [AirbyteHttpRequestFieldExtractor] class.
 */
internal class AirbyteHttpRequestFieldExtractorTest {
  private lateinit var airbyteHttpRequestFieldExtractor: AirbyteHttpRequestFieldExtractor

  @BeforeEach
  fun setup() {
    airbyteHttpRequestFieldExtractor = AirbyteHttpRequestFieldExtractor()
  }

  @Test
  fun testExtractionUUID() {
    val match = UUID.randomUUID()
    val other = UUID.randomUUID()
    val idFieldName: String = SOME_ID
    val content = mapOf(idFieldName to match.toString(), OTHER_ID to other.toString())
    val contentAsJson = jsonNode(content)

    val extractedId: Optional<String> = airbyteHttpRequestFieldExtractor.extractId(contentAsJson, idFieldName)

    Assertions.assertTrue(extractedId.isPresent())
    Assertions.assertEquals(match, UUID.fromString(extractedId.get()))
  }

  @Test
  fun testExtractionNonUUID() {
    val match = 12345L
    val other = Long.MAX_VALUE
    val idFieldName: String = SOME_ID
    val content = mapOf(idFieldName to match.toString(), OTHER_ID to other.toString())
    val contentAsJson = jsonNode(content)

    val extractedId: Optional<String> = airbyteHttpRequestFieldExtractor.extractId(contentAsJson, idFieldName)

    Assertions.assertTrue(extractedId.isPresent())
    Assertions.assertEquals(match, extractedId.get().toLong())
  }

  @Test
  fun testExtractionWithEmptyContent() {
    val extractedId: Optional<String> = airbyteHttpRequestFieldExtractor.extractId(null, SOME_ID)
    Assertions.assertTrue(extractedId.isEmpty())
  }

  @Test
  fun testExtractionWithMissingField() {
    val match = UUID.randomUUID()
    val other = UUID.randomUUID()
    val idFieldName: String = SOME_ID
    val content = mapOf(idFieldName to match.toString(), OTHER_ID to other.toString())
    val contentAsJson = jsonNode(content)

    val extractedId: Optional<String> = airbyteHttpRequestFieldExtractor.extractId(contentAsJson, "unknownFieldId")

    Assertions.assertTrue(extractedId.isEmpty())
  }

  @Test
  fun testExtractionWithNoMatch() {
    val idFieldName: String = SOME_ID
    val content = mapOf(OTHER_ID to UUID.randomUUID().toString())
    val contentAsJson = jsonNode(content)

    val extractedId: Optional<String> = airbyteHttpRequestFieldExtractor.extractId(contentAsJson, idFieldName)

    Assertions.assertTrue(extractedId.isEmpty())
  }

  @Test
  fun testWorkspaceIdsExtractionWithNullWorkspaceIds() {
    val idFieldName: String = WORKSPACE_IDS_FIELD_NAME
    val other = UUID.randomUUID()

    val content = mapOf(OTHER_ID to other.toString())
    val contentAsJson = jsonNode(content)

    val extractedId: Optional<String> = airbyteHttpRequestFieldExtractor.extractId(contentAsJson, idFieldName)

    Assertions.assertTrue(extractedId.isEmpty())
  }

  @Test
  fun testWorkspaceIdsExtraction() {
    val idFieldName: String = WORKSPACE_IDS_FIELD_NAME
    val valueList = listOf(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID())
    val valueString = String.format("[\"%s\",\"%s\",\"%s\"]", valueList[0], valueList[1], valueList[2])
    val other = UUID.randomUUID()

    val content = mapOf(WORKSPACE_IDS_FIELD_NAME to valueList, OTHER_ID to other.toString())
    val contentAsJson = jsonNode(content)

    val extractedId: Optional<String> = airbyteHttpRequestFieldExtractor.extractId(contentAsJson, idFieldName)

    Assertions.assertEquals(extractedId, Optional.of(valueString))
  }

  // Just want to make sure that coverage exists
  @Test
  fun testContentToJson() {
    val contentAsString = "{\"key\":\"value\"}"
    val contentAsJson: Optional<JsonNode> = airbyteHttpRequestFieldExtractor.contentToJson(contentAsString)

    Assertions.assertTrue(contentAsJson.isPresent())
    Assertions.assertEquals(contentAsJson.get().get("key").asText(), "value")

    val invalidContentAsString = "invalid json"
    val invalidContentAsJson: Optional<JsonNode> = airbyteHttpRequestFieldExtractor.contentToJson(invalidContentAsString)

    Assertions.assertTrue(invalidContentAsJson.isEmpty())

    val emptyContentAsString = ""
    val emptyContentAsJson: Optional<JsonNode> = airbyteHttpRequestFieldExtractor.contentToJson(emptyContentAsString)

    Assertions.assertTrue(emptyContentAsJson.isEmpty())
  }

  companion object {
    private const val OTHER_ID = "other_id"
    private const val SOME_ID = "some_id"
  }
}
