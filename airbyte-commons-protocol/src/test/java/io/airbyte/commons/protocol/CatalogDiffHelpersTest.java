/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.io.Resources;
import io.airbyte.commons.protocol.transform_models.FieldTransform;
import io.airbyte.commons.protocol.transform_models.StreamTransform;
import io.airbyte.commons.protocol.transform_models.StreamTransformType;
import io.airbyte.commons.protocol.transform_models.UpdateFieldSchemaTransform;
import io.airbyte.commons.protocol.transform_models.UpdateStreamTransform;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.DestinationSyncMode;
import io.airbyte.protocol.models.Jsons;
import io.airbyte.protocol.models.StreamDescriptor;
import io.airbyte.protocol.models.SyncMode;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
class CatalogDiffHelpersTest {

  // handy for debugging test only.
  private static final Comparator<StreamTransform> STREAM_TRANSFORM_COMPARATOR =
      Comparator.comparing(StreamTransform::getTransformType);
  private static final String CAD = "CAD";
  private static final String ITEMS = "items";
  private static final String SOME_ARRAY = "someArray";
  private static final String PROPERTIES = "properties";
  private static final String USERS = "users";
  private static final String DATE = "date";
  private static final String SALES = "sales";
  private static final String COMPANIES_VALID = "diffs/companies_schema.json";
  private static final String COMPANIES_INVALID = "diffs/companies_schema_invalid.json";
  private static final String VALID_SCHEMA_JSON = "diffs/valid_schema.json";

  @SuppressWarnings("UnstableApiUsage")
  private String readResource(final String name) throws IOException {
    final URL resource = Resources.getResource(name);
    return Resources.toString(resource, StandardCharsets.UTF_8);
  }

  @Test
  void testGetFieldNames() throws IOException {
    final JsonNode node = Jsons.deserialize(readResource(VALID_SCHEMA_JSON));
    final Set<String> actualFieldNames = CatalogDiffHelpers.getAllFieldNames(node);
    final List<String> expectedFieldNames =
        List.of("id", CAD, "DKK", "HKD", "HUF", "ISK", "PHP", DATE, "nestedkey", "somekey", "something", "something2", "文", SOME_ARRAY, ITEMS,
            "oldName");

    // sort so that the diff is easier to read.
    assertEquals(expectedFieldNames.stream().sorted().toList(), actualFieldNames.stream().sorted().toList());
  }

  @Test
  void testGetCatalogDiff() throws IOException {
    final JsonNode schema1 = Jsons.deserialize(readResource(VALID_SCHEMA_JSON));
    final JsonNode schema2 = Jsons.deserialize(readResource("diffs/valid_schema2.json"));
    final AirbyteCatalog catalog1 = new AirbyteCatalog().withStreams(List.of(
        new AirbyteStream().withName(USERS).withJsonSchema(schema1),
        new AirbyteStream().withName("accounts").withJsonSchema(Jsons.emptyObject())));
    final AirbyteCatalog catalog2 = new AirbyteCatalog().withStreams(List.of(
        new AirbyteStream().withName(USERS).withJsonSchema(schema2),
        new AirbyteStream().withName(SALES).withJsonSchema(Jsons.emptyObject())));

    final ConfiguredAirbyteCatalog configuredAirbyteCatalog = new ConfiguredAirbyteCatalog().withStreams(List.of(
        new ConfiguredAirbyteStream().withStream(new AirbyteStream().withName(USERS).withJsonSchema(schema2)).withSyncMode(SyncMode.FULL_REFRESH),
        new ConfiguredAirbyteStream().withStream(new AirbyteStream().withName(SALES).withJsonSchema(Jsons.emptyObject()))
            .withSyncMode(SyncMode.FULL_REFRESH)));

    final Set<StreamTransform> actualDiff = CatalogDiffHelpers.getCatalogDiff(catalog1, catalog2, configuredAirbyteCatalog);
    final List<StreamTransform> expectedDiff = Stream.of(
        StreamTransform.createAddStreamTransform(new StreamDescriptor().withName(SALES)),
        StreamTransform.createRemoveStreamTransform(new StreamDescriptor().withName("accounts")),
        StreamTransform.createUpdateStreamTransform(new StreamDescriptor().withName(USERS), new UpdateStreamTransform(Set.of(
            FieldTransform.createAddFieldTransform(List.of("COD"), schema2.get(PROPERTIES).get("COD")),
            FieldTransform.createAddFieldTransform(List.of("somePreviouslyInvalidField"), schema2.get(PROPERTIES).get("somePreviouslyInvalidField")),
            FieldTransform.createRemoveFieldTransform(List.of("something2"), schema1.get(PROPERTIES).get("something2"), false),
            FieldTransform.createRemoveFieldTransform(List.of("HKD"), schema1.get(PROPERTIES).get("HKD"), false),
            FieldTransform.createUpdateFieldTransform(List.of(CAD), new UpdateFieldSchemaTransform(
                schema1.get(PROPERTIES).get(CAD),
                schema2.get(PROPERTIES).get(CAD))),
            FieldTransform.createUpdateFieldTransform(List.of(SOME_ARRAY), new UpdateFieldSchemaTransform(
                schema1.get(PROPERTIES).get(SOME_ARRAY),
                schema2.get(PROPERTIES).get(SOME_ARRAY))),
            FieldTransform.createUpdateFieldTransform(List.of(SOME_ARRAY, ITEMS), new UpdateFieldSchemaTransform(
                schema1.get(PROPERTIES).get(SOME_ARRAY).get(ITEMS),
                schema2.get(PROPERTIES).get(SOME_ARRAY).get(ITEMS))),
            FieldTransform.createRemoveFieldTransform(List.of(SOME_ARRAY, ITEMS, "oldName"),
                schema1.get(PROPERTIES).get(SOME_ARRAY).get(ITEMS).get(PROPERTIES).get("oldName"), false),
            FieldTransform.createAddFieldTransform(List.of(SOME_ARRAY, ITEMS, "newName"),
                schema2.get(PROPERTIES).get(SOME_ARRAY).get(ITEMS).get(PROPERTIES).get("newName"))))))
        .sorted(STREAM_TRANSFORM_COMPARATOR)
        .toList();

    Assertions.assertThat(actualDiff).containsAll(expectedDiff);
  }

  @Test
  void testGetFullyQualifiedFieldNamesWithTypes() throws IOException {
    CatalogDiffHelpers.getFullyQualifiedFieldNamesWithTypes(
        Jsons.deserialize(readResource(COMPANIES_VALID))).stream().collect(
            () -> new HashMap<>(),
            CatalogDiffHelpers::collectInHashMap,
            CatalogDiffHelpers::combineAccumulator);
  }

  @Test
  void testGetFullyQualifiedFieldNamesWithTypesOnInvalidSchema() throws IOException {
    final Map<List<String>, JsonNode> resultField = CatalogDiffHelpers.getFullyQualifiedFieldNamesWithTypes(
        Jsons.deserialize(readResource(COMPANIES_INVALID))).stream().collect(
            () -> new HashMap<>(),
            CatalogDiffHelpers::collectInHashMap,
            CatalogDiffHelpers::combineAccumulator);

    Assertions.assertThat(resultField)
        .contains(
            Map.entry(
                List.of("tags", "tags", "items"),
                CatalogDiffHelpers.DUPLICATED_SCHEMA));
  }

  @Test
  void testGetCatalogDiffWithInvalidSchema() throws IOException {
    final JsonNode schema1 = Jsons.deserialize(readResource(COMPANIES_INVALID));
    final JsonNode schema2 = Jsons.deserialize(readResource(COMPANIES_VALID));
    final AirbyteCatalog catalog1 = new AirbyteCatalog().withStreams(List.of(
        new AirbyteStream().withName(USERS).withJsonSchema(schema1)));
    final AirbyteCatalog catalog2 = new AirbyteCatalog().withStreams(List.of(
        new AirbyteStream().withName(USERS).withJsonSchema(schema2)));

    final ConfiguredAirbyteCatalog configuredAirbyteCatalog = new ConfiguredAirbyteCatalog().withStreams(List.of(
        new ConfiguredAirbyteStream().withStream(new AirbyteStream().withName(USERS).withJsonSchema(schema2)).withSyncMode(SyncMode.FULL_REFRESH),
        new ConfiguredAirbyteStream().withStream(new AirbyteStream().withName(SALES).withJsonSchema(Jsons.emptyObject()))
            .withSyncMode(SyncMode.FULL_REFRESH)));

    final Set<StreamTransform> actualDiff = CatalogDiffHelpers.getCatalogDiff(catalog1, catalog2, configuredAirbyteCatalog);

    Assertions.assertThat(actualDiff).hasSize(1);
    Assertions.assertThat(actualDiff).first()
        .has(new Condition<StreamTransform>(streamTransform -> streamTransform.getTransformType() == StreamTransformType.UPDATE_STREAM,
            "Check update"));
  }

  @Test
  void testGetCatalogDiffWithBothInvalidSchema() throws IOException {
    final JsonNode schema1 = Jsons.deserialize(readResource(COMPANIES_INVALID));
    final JsonNode schema2 = Jsons.deserialize(readResource(COMPANIES_INVALID));
    final AirbyteCatalog catalog1 = new AirbyteCatalog().withStreams(List.of(
        new AirbyteStream().withName(USERS).withJsonSchema(schema1)));
    final AirbyteCatalog catalog2 = new AirbyteCatalog().withStreams(List.of(
        new AirbyteStream().withName(USERS).withJsonSchema(schema2)));

    final ConfiguredAirbyteCatalog configuredAirbyteCatalog = new ConfiguredAirbyteCatalog().withStreams(List.of(
        new ConfiguredAirbyteStream().withStream(new AirbyteStream().withName(USERS).withJsonSchema(schema2)).withSyncMode(SyncMode.FULL_REFRESH),
        new ConfiguredAirbyteStream().withStream(new AirbyteStream().withName(SALES).withJsonSchema(Jsons.emptyObject()))
            .withSyncMode(SyncMode.FULL_REFRESH)));

    final Set<StreamTransform> actualDiff = CatalogDiffHelpers.getCatalogDiff(catalog1, catalog2, configuredAirbyteCatalog);

    Assertions.assertThat(actualDiff).hasSize(0);
  }

  @Test
  void testCatalogDiffWithBreakingChanges() throws IOException {
    final JsonNode schema1 = Jsons.deserialize(readResource(VALID_SCHEMA_JSON));
    final JsonNode breakingSchema = Jsons.deserialize(readResource("diffs/breaking_change_schema.json"));
    final AirbyteCatalog catalog1 = new AirbyteCatalog().withStreams(List.of(
        new AirbyteStream().withName(USERS).withJsonSchema(schema1)));
    final AirbyteCatalog catalog2 = new AirbyteCatalog().withStreams(List.of(
        new AirbyteStream().withName(USERS).withJsonSchema(breakingSchema)));

    final ConfiguredAirbyteCatalog configuredAirbyteCatalog = new ConfiguredAirbyteCatalog().withStreams(List.of(
        new ConfiguredAirbyteStream().withStream(new AirbyteStream().withName(USERS).withJsonSchema(schema1)).withSyncMode(SyncMode.INCREMENTAL)
            .withCursorField(List.of(DATE)).withDestinationSyncMode(DestinationSyncMode.APPEND_DEDUP).withPrimaryKey(List.of(List.of("id")))));

    final Set<StreamTransform> diff = CatalogDiffHelpers.getCatalogDiff(catalog1, catalog2, configuredAirbyteCatalog);

    final List<StreamTransform> expectedDiff = Stream.of(
        StreamTransform.createUpdateStreamTransform(new StreamDescriptor().withName(USERS), new UpdateStreamTransform(Set.of(
            FieldTransform.createRemoveFieldTransform(List.of(DATE), schema1.get(PROPERTIES).get(DATE), true),
            FieldTransform.createRemoveFieldTransform(List.of("id"), schema1.get(PROPERTIES).get("id"), true)))))
        .toList();

    Assertions.assertThat(diff).containsAll(expectedDiff);
  }

  @Test
  void testCatalogDiffWithoutStreamConfig() throws IOException {
    final JsonNode schema1 = Jsons.deserialize(readResource(VALID_SCHEMA_JSON));
    final JsonNode breakingSchema = Jsons.deserialize(readResource("diffs/breaking_change_schema.json"));
    final AirbyteCatalog catalog1 = new AirbyteCatalog().withStreams(List.of(
        new AirbyteStream().withName(USERS).withJsonSchema(schema1)));
    final AirbyteCatalog catalog2 = new AirbyteCatalog().withStreams(List.of(
        new AirbyteStream().withName(USERS).withJsonSchema(breakingSchema)));

    final ConfiguredAirbyteCatalog configuredAirbyteCatalog = new ConfiguredAirbyteCatalog().withStreams(List.of(
        new ConfiguredAirbyteStream().withStream(new AirbyteStream().withName(SALES).withJsonSchema(schema1)).withSyncMode(SyncMode.INCREMENTAL)
            .withCursorField(List.of(DATE)).withDestinationSyncMode(DestinationSyncMode.APPEND_DEDUP).withPrimaryKey(List.of(List.of("id")))));

    final Set<StreamTransform> diff = CatalogDiffHelpers.getCatalogDiff(catalog1, catalog2, configuredAirbyteCatalog);

    // configuredCatalog is for a different stream, so no diff should be found
    Assertions.assertThat(diff).hasSize(0);
  }

  @Test
  void testCatalogDiffStreamChangeWithNoFieldTransform() throws IOException {
    final JsonNode schema1 = Jsons.deserialize(readResource(VALID_SCHEMA_JSON));

    final AirbyteCatalog catalog1 = new AirbyteCatalog().withStreams(List.of(
        new AirbyteStream().withName(USERS).withJsonSchema(schema1),
        new AirbyteStream().withName(SALES).withJsonSchema(schema1)));
    final AirbyteCatalog catalog2 = new AirbyteCatalog().withStreams(List.of(
        new AirbyteStream().withName(USERS).withJsonSchema(schema1).withSourceDefinedPrimaryKey(List.of(List.of("id")))));

    final ConfiguredAirbyteCatalog configuredAirbyteCatalog = new ConfiguredAirbyteCatalog().withStreams(List.of(
        new ConfiguredAirbyteStream().withStream(new AirbyteStream().withName(USERS).withJsonSchema(schema1)).withSyncMode(SyncMode.FULL_REFRESH),
        new ConfiguredAirbyteStream().withStream(new AirbyteStream().withName(SALES).withJsonSchema(schema1))
            .withSyncMode(SyncMode.FULL_REFRESH)));

    final Set<StreamTransform> actualDiff = CatalogDiffHelpers.getCatalogDiff(catalog1, catalog2, configuredAirbyteCatalog);

    final List<StreamTransform> expectedDiff = Stream.of(
        StreamTransform.createRemoveStreamTransform(new StreamDescriptor().withName(SALES)))
        .toList();

    Assertions.assertThat(actualDiff).containsExactlyInAnyOrderElementsOf(expectedDiff);
  }

}
