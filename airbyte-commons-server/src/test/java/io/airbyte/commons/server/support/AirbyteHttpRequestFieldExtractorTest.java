/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support;

import static io.airbyte.commons.server.support.AuthenticationFields.WORKSPACE_IDS_FIELD_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.json.Jsons;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test suite for the {@link AirbyteHttpRequestFieldExtractor} class.
 */
class AirbyteHttpRequestFieldExtractorTest {

  private static final String OTHER_ID = "other_id";
  private static final String SOME_ID = "some_id";

  private AirbyteHttpRequestFieldExtractor airbyteHttpRequestFieldExtractor;

  @BeforeEach
  void setup() {
    airbyteHttpRequestFieldExtractor = new AirbyteHttpRequestFieldExtractor();
  }

  @Test
  void testExtractionUUID() {
    final UUID match = UUID.randomUUID();
    final UUID other = UUID.randomUUID();
    final String idFieldName = SOME_ID;
    final Map<String, String> content = Map.of(idFieldName, match.toString(), OTHER_ID, other.toString());
    final JsonNode contentAsJson = Jsons.jsonNode(content);

    final Optional<String> extractedId = airbyteHttpRequestFieldExtractor.extractId(contentAsJson, idFieldName);

    assertTrue(extractedId.isPresent());
    assertEquals(match, UUID.fromString(extractedId.get()));
  }

  @Test
  void testExtractionNonUUID() {
    final Long match = 12345L;
    final Long other = Long.MAX_VALUE;
    final String idFieldName = SOME_ID;
    final Map<String, String> content = Map.of(idFieldName, match.toString(), OTHER_ID, other.toString());
    final JsonNode contentAsJson = Jsons.jsonNode(content);

    final Optional<String> extractedId = airbyteHttpRequestFieldExtractor.extractId(contentAsJson, idFieldName);

    assertTrue(extractedId.isPresent());
    assertEquals(match, Long.valueOf(extractedId.get()));
  }

  @Test
  void testExtractionWithEmptyContent() {
    final Optional<String> extractedId = airbyteHttpRequestFieldExtractor.extractId(null, SOME_ID);
    assertTrue(extractedId.isEmpty());
  }

  @Test
  void testExtractionWithMissingField() {
    final UUID match = UUID.randomUUID();
    final UUID other = UUID.randomUUID();
    final String idFieldName = SOME_ID;
    final Map<String, String> content = Map.of(idFieldName, match.toString(), OTHER_ID, other.toString());
    final JsonNode contentAsJson = Jsons.jsonNode(content);

    final Optional<String> extractedId = airbyteHttpRequestFieldExtractor.extractId(contentAsJson, "unknownFieldId");

    assertTrue(extractedId.isEmpty());
  }

  @Test
  void testExtractionWithNoMatch() {
    final String idFieldName = SOME_ID;
    final Map<String, String> content = Map.of(OTHER_ID, UUID.randomUUID().toString());
    final JsonNode contentAsJson = Jsons.jsonNode(content);

    final Optional<String> extractedId = airbyteHttpRequestFieldExtractor.extractId(contentAsJson, idFieldName);

    assertTrue(extractedId.isEmpty());
  }

  @Test
  void testWorkspaceIdsExtractionWithNullWorkspaceIds() {
    final String idFieldName = WORKSPACE_IDS_FIELD_NAME;
    final UUID other = UUID.randomUUID();

    final Map<String, String> content = Map.of(OTHER_ID, other.toString());
    final JsonNode contentAsJson = Jsons.jsonNode(content);

    final Optional<String> extractedId = airbyteHttpRequestFieldExtractor.extractId(contentAsJson, idFieldName);

    assertTrue(extractedId.isEmpty());
  }

  @Test
  void testWorkspaceIdsExtraction() {
    final String idFieldName = WORKSPACE_IDS_FIELD_NAME;
    final List<UUID> valueList = List.of(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
    final String valueString = String.format("[\"%s\",\"%s\",\"%s\"]", valueList.get(0), valueList.get(1), valueList.get(2));
    final UUID other = UUID.randomUUID();

    final Map<String, ?> content = Map.of(WORKSPACE_IDS_FIELD_NAME, valueList, OTHER_ID, other.toString());
    final JsonNode contentAsJson = Jsons.jsonNode(content);

    final Optional<String> extractedId = airbyteHttpRequestFieldExtractor.extractId(contentAsJson, idFieldName);

    assertEquals(extractedId, Optional.of(valueString));
  }

  // Just want to make sure that coverage exists
  @Test
  void testContentToJson() {
    final String contentAsString = "{\"key\":\"value\"}";
    final Optional<JsonNode> contentAsJson = airbyteHttpRequestFieldExtractor.contentToJson(contentAsString);

    assertTrue(contentAsJson.isPresent());
    assertEquals(contentAsJson.get().get("key").asText(), "value");

    final String invalidContentAsString = "invalid json";
    final Optional<JsonNode> invalidContentAsJson = airbyteHttpRequestFieldExtractor.contentToJson(invalidContentAsString);

    assertTrue(invalidContentAsJson.isEmpty());

    final String emptyContentAsString = "";
    final Optional<JsonNode> emptyContentAsJson = airbyteHttpRequestFieldExtractor.contentToJson(emptyContentAsString);

    assertTrue(emptyContentAsJson.isEmpty());
  }

}
