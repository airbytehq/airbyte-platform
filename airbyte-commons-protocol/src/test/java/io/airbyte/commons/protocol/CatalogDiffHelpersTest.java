/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.io.Resources;
import io.airbyte.commons.protocol.transformmodels.FieldTransform;
import io.airbyte.commons.protocol.transformmodels.StreamAttributeTransform;
import io.airbyte.commons.protocol.transformmodels.StreamTransform;
import io.airbyte.commons.protocol.transformmodels.StreamTransformType;
import io.airbyte.commons.protocol.transformmodels.UpdateFieldSchemaTransform;
import io.airbyte.commons.protocol.transformmodels.UpdateStreamTransform;
import io.airbyte.config.AirbyteStream;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.ConfiguredAirbyteStream;
import io.airbyte.config.DestinationSyncMode;
import io.airbyte.config.StreamDescriptor;
import io.airbyte.config.SyncMode;
import io.airbyte.config.helpers.ProtocolConverters;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.Jsons;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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
  private static final List<List<String>> ID_PK = List.of(List.of("id"));
  private static final List<List<String>> DATE_PK = List.of(List.of(DATE));
  private static final List<List<String>> COMPOSITE_PK = List.of(List.of("id"), List.of(DATE));
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
        new io.airbyte.protocol.models.AirbyteStream().withName(USERS).withJsonSchema(schema1),
        new io.airbyte.protocol.models.AirbyteStream().withName("accounts").withJsonSchema(Jsons.emptyObject())));
    final AirbyteCatalog catalog2 = new AirbyteCatalog().withStreams(List.of(
        new io.airbyte.protocol.models.AirbyteStream().withName(USERS).withJsonSchema(schema2),
        new io.airbyte.protocol.models.AirbyteStream().withName(SALES).withJsonSchema(Jsons.emptyObject())));

    final ConfiguredAirbyteCatalog configuredAirbyteCatalog = new ConfiguredAirbyteCatalog().withStreams(List.of(
        new ConfiguredAirbyteStream(new AirbyteStream(USERS, schema2, List.of(SyncMode.FULL_REFRESH)), SyncMode.FULL_REFRESH,
            DestinationSyncMode.APPEND),
        new ConfiguredAirbyteStream(new AirbyteStream(SALES, Jsons.emptyObject(), List.of(SyncMode.FULL_REFRESH)), SyncMode.FULL_REFRESH,
            DestinationSyncMode.APPEND)));

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
                schema2.get(PROPERTIES).get(SOME_ARRAY).get(ITEMS).get(PROPERTIES).get("newName"))),
            Set.of())))
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
        new io.airbyte.protocol.models.AirbyteStream().withName(USERS).withJsonSchema(schema1)));
    final AirbyteCatalog catalog2 = new AirbyteCatalog().withStreams(List.of(
        new io.airbyte.protocol.models.AirbyteStream().withName(USERS).withJsonSchema(schema2)));

    final ConfiguredAirbyteCatalog configuredAirbyteCatalog = new ConfiguredAirbyteCatalog().withStreams(List.of(
        new ConfiguredAirbyteStream(new AirbyteStream(USERS, schema2, List.of(SyncMode.FULL_REFRESH)), SyncMode.FULL_REFRESH,
            DestinationSyncMode.APPEND),
        new ConfiguredAirbyteStream(new AirbyteStream(SALES, Jsons.emptyObject(), List.of(SyncMode.FULL_REFRESH)), SyncMode.FULL_REFRESH,
            DestinationSyncMode.APPEND)));

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
        new io.airbyte.protocol.models.AirbyteStream().withName(USERS).withJsonSchema(schema1)));
    final AirbyteCatalog catalog2 = new AirbyteCatalog().withStreams(List.of(
        new io.airbyte.protocol.models.AirbyteStream().withName(USERS).withJsonSchema(schema2)));

    final ConfiguredAirbyteCatalog configuredAirbyteCatalog = new ConfiguredAirbyteCatalog().withStreams(List.of(
        new ConfiguredAirbyteStream(new AirbyteStream(USERS, schema2, List.of(SyncMode.FULL_REFRESH)), SyncMode.FULL_REFRESH,
            DestinationSyncMode.APPEND),
        new ConfiguredAirbyteStream(new AirbyteStream(SALES, Jsons.emptyObject(), List.of(SyncMode.FULL_REFRESH)), SyncMode.FULL_REFRESH,
            DestinationSyncMode.APPEND)));

    final Set<StreamTransform> actualDiff = CatalogDiffHelpers.getCatalogDiff(catalog1, catalog2, configuredAirbyteCatalog);

    Assertions.assertThat(actualDiff).hasSize(0);
  }

  @Test
  void testCatalogDiffWithBreakingChanges() throws IOException {
    final JsonNode schema1 = Jsons.deserialize(readResource(VALID_SCHEMA_JSON));
    final JsonNode breakingSchema = Jsons.deserialize(readResource("diffs/breaking_change_schema.json"));
    final AirbyteCatalog catalog1 = new AirbyteCatalog().withStreams(List.of(
        new io.airbyte.protocol.models.AirbyteStream().withName(USERS).withJsonSchema(schema1)));
    final AirbyteCatalog catalog2 = new AirbyteCatalog().withStreams(List.of(
        new io.airbyte.protocol.models.AirbyteStream().withName(USERS).withJsonSchema(breakingSchema)));

    final ConfiguredAirbyteCatalog configuredAirbyteCatalog = new ConfiguredAirbyteCatalog().withStreams(List.of(
        new ConfiguredAirbyteStream(new AirbyteStream(USERS, schema1, List.of(SyncMode.INCREMENTAL)), SyncMode.INCREMENTAL,
            DestinationSyncMode.APPEND_DEDUP)
                .withCursorField(List.of(DATE)).withPrimaryKey(List.of(List.of("id")))));

    final Set<StreamTransform> diff = CatalogDiffHelpers.getCatalogDiff(catalog1, catalog2, configuredAirbyteCatalog);

    final List<StreamTransform> expectedDiff = Stream.of(
        StreamTransform.createUpdateStreamTransform(new StreamDescriptor().withName(USERS), new UpdateStreamTransform(Set.of(
            FieldTransform.createRemoveFieldTransform(List.of(DATE), schema1.get(PROPERTIES).get(DATE), true),
            FieldTransform.createRemoveFieldTransform(List.of("id"), schema1.get(PROPERTIES).get("id"), true)), Set.of())))
        .toList();

    Assertions.assertThat(diff).containsAll(expectedDiff);
  }

  @Test
  void testCatalogDiffWithoutStreamConfig() throws IOException {
    final JsonNode schema1 = Jsons.deserialize(readResource(VALID_SCHEMA_JSON));
    final JsonNode breakingSchema = Jsons.deserialize(readResource("diffs/breaking_change_schema.json"));
    final AirbyteCatalog catalog1 = new AirbyteCatalog().withStreams(List.of(
        new io.airbyte.protocol.models.AirbyteStream().withName(USERS).withJsonSchema(schema1)));
    final AirbyteCatalog catalog2 = new AirbyteCatalog().withStreams(List.of(
        new io.airbyte.protocol.models.AirbyteStream().withName(USERS).withJsonSchema(breakingSchema)));

    final ConfiguredAirbyteCatalog configuredAirbyteCatalog = new ConfiguredAirbyteCatalog().withStreams(List.of(
        new ConfiguredAirbyteStream(new AirbyteStream(SALES, schema1, List.of(SyncMode.INCREMENTAL)), SyncMode.INCREMENTAL,
            DestinationSyncMode.APPEND_DEDUP)
                .withCursorField(List.of(DATE)).withPrimaryKey(List.of(List.of("id")))));

    final Set<StreamTransform> diff = CatalogDiffHelpers.getCatalogDiff(catalog1, catalog2, configuredAirbyteCatalog);

    // configuredCatalog is for a different stream, so no diff should be found
    Assertions.assertThat(diff).hasSize(0);
  }

  @Test
  void testCatalogDiffStreamChangeWithNoTransforms() throws IOException {
    final JsonNode schema1 = Jsons.deserialize(readResource(VALID_SCHEMA_JSON));

    final AirbyteCatalog catalog1 = new AirbyteCatalog().withStreams(List.of(
        ProtocolConverters
            .toProtocol(new AirbyteStream(USERS, schema1, List.of(SyncMode.FULL_REFRESH)).withSourceDefinedPrimaryKey(List.of(List.of("id")))),
        ProtocolConverters.toProtocol(new AirbyteStream(SALES, schema1, List.of(SyncMode.FULL_REFRESH)))));
    final AirbyteCatalog catalog2 = new AirbyteCatalog().withStreams(List.of(
        ProtocolConverters
            .toProtocol(new AirbyteStream(USERS, schema1, List.of(SyncMode.FULL_REFRESH)).withSourceDefinedPrimaryKey(List.of(List.of("id"))))));

    final ConfiguredAirbyteCatalog configuredAirbyteCatalog = new ConfiguredAirbyteCatalog().withStreams(List.of(
        new ConfiguredAirbyteStream(new AirbyteStream(USERS, schema1, List.of(SyncMode.FULL_REFRESH)), SyncMode.FULL_REFRESH,
            DestinationSyncMode.APPEND),
        new ConfiguredAirbyteStream(new AirbyteStream(SALES, schema1, List.of(SyncMode.FULL_REFRESH)), SyncMode.FULL_REFRESH,
            DestinationSyncMode.APPEND)));

    final Set<StreamTransform> actualDiff = CatalogDiffHelpers.getCatalogDiff(catalog1, catalog2, configuredAirbyteCatalog);

    final List<StreamTransform> expectedDiff = Stream.of(
        StreamTransform.createRemoveStreamTransform(new StreamDescriptor().withName(SALES)))
        .toList();

    Assertions.assertThat(actualDiff).containsExactlyElementsOf(expectedDiff);
  }

  private static Stream<Arguments> testCatalogDiffWithSourceDefinedPrimaryKeyChangeMethodSource() {
    return Stream.of(
        // Should be breaking in DE-DUP mode if the previous PK is not the new source-defined PK
        Arguments.of(DestinationSyncMode.APPEND_DEDUP, ID_PK, ID_PK, DATE_PK, true),
        Arguments.of(DestinationSyncMode.APPEND_DEDUP, ID_PK, List.of(), DATE_PK, true),
        Arguments.of(DestinationSyncMode.APPEND_DEDUP, COMPOSITE_PK, COMPOSITE_PK, ID_PK, true),
        Arguments.of(DestinationSyncMode.OVERWRITE_DEDUP, ID_PK, ID_PK, DATE_PK, true),
        Arguments.of(DestinationSyncMode.OVERWRITE_DEDUP, ID_PK, List.of(), DATE_PK, true),
        Arguments.of(DestinationSyncMode.OVERWRITE_DEDUP, COMPOSITE_PK, COMPOSITE_PK, ID_PK, true),

        // Should not be breaking in DE-DUP mode if the previous and new source-defined PK contain the
        // fields in a different order
        Arguments.of(DestinationSyncMode.APPEND_DEDUP, COMPOSITE_PK, COMPOSITE_PK, COMPOSITE_PK.reversed(), false),
        Arguments.of(DestinationSyncMode.OVERWRITE_DEDUP, COMPOSITE_PK, COMPOSITE_PK, COMPOSITE_PK.reversed(), false),

        // Should not be breaking in other sync modes
        Arguments.of(DestinationSyncMode.APPEND, ID_PK, ID_PK, DATE_PK, false),
        Arguments.of(DestinationSyncMode.OVERWRITE, ID_PK, ID_PK, DATE_PK, false),
        Arguments.of(DestinationSyncMode.APPEND, ID_PK, List.of(), DATE_PK, false),
        Arguments.of(DestinationSyncMode.OVERWRITE, ID_PK, List.of(), DATE_PK, false),
        Arguments.of(DestinationSyncMode.APPEND, COMPOSITE_PK, COMPOSITE_PK, ID_PK, false),
        Arguments.of(DestinationSyncMode.OVERWRITE, COMPOSITE_PK, COMPOSITE_PK, ID_PK, false),

        // Should not be breaking if added source-defined PK is already the manually set PK
        Arguments.of(DestinationSyncMode.APPEND, ID_PK, List.of(), ID_PK, false),
        Arguments.of(DestinationSyncMode.APPEND_DEDUP, ID_PK, List.of(), ID_PK, false),
        Arguments.of(DestinationSyncMode.OVERWRITE, ID_PK, List.of(), ID_PK, false),
        Arguments.of(DestinationSyncMode.OVERWRITE_DEDUP, ID_PK, List.of(), ID_PK, false),

        // Removing source-defined PK should not be breaking
        Arguments.of(DestinationSyncMode.APPEND, ID_PK, ID_PK, List.of(), false),
        Arguments.of(DestinationSyncMode.APPEND_DEDUP, ID_PK, ID_PK, List.of(), false),
        Arguments.of(DestinationSyncMode.OVERWRITE, ID_PK, ID_PK, List.of(), false),
        Arguments.of(DestinationSyncMode.OVERWRITE_DEDUP, ID_PK, ID_PK, List.of(), false));
  }

  @ParameterizedTest
  @MethodSource("testCatalogDiffWithSourceDefinedPrimaryKeyChangeMethodSource")
  void testCatalogDiffWithSourceDefinedPrimaryKeyChange(final DestinationSyncMode destSyncMode,
                                                        final List<List<String>> configuredPK,
                                                        final List<List<String>> prevSourcePK,
                                                        final List<List<String>> newSourcePK,
                                                        final boolean isBreaking)
      throws IOException {
    final JsonNode schema = Jsons.deserialize(readResource(VALID_SCHEMA_JSON));

    final AirbyteStream stream = new AirbyteStream(USERS, schema, List.of(SyncMode.FULL_REFRESH)).withSourceDefinedPrimaryKey(prevSourcePK);
    final AirbyteStream refreshedStream = new AirbyteStream(USERS, schema, List.of(SyncMode.FULL_REFRESH)).withSourceDefinedPrimaryKey(newSourcePK);

    final AirbyteCatalog initialCatalog = new AirbyteCatalog().withStreams(List.of(ProtocolConverters.toProtocol(stream)));
    final AirbyteCatalog refreshedCatalog = new AirbyteCatalog().withStreams(List.of(ProtocolConverters.toProtocol(refreshedStream)));

    final ConfiguredAirbyteStream configuredStream = new ConfiguredAirbyteStream(stream, SyncMode.INCREMENTAL, destSyncMode)
        .withPrimaryKey(configuredPK);

    final ConfiguredAirbyteCatalog configuredCatalog = new ConfiguredAirbyteCatalog().withStreams(List.of(configuredStream));

    final Set<StreamTransform> actualDiff = CatalogDiffHelpers.getCatalogDiff(initialCatalog, refreshedCatalog, configuredCatalog);

    final List<StreamTransform> expectedDiff = List.of(
        StreamTransform.createUpdateStreamTransform(new StreamDescriptor().withName(USERS),
            new UpdateStreamTransform(Set.of(),
                Set.of(StreamAttributeTransform.createUpdatePrimaryKeyTransform(prevSourcePK, newSourcePK, isBreaking)))));

    Assertions.assertThat(actualDiff).containsExactlyElementsOf(expectedDiff);
  }

}
