/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.io.Resources
import io.airbyte.commons.protocol.CatalogDiffHelpers.getAllFieldNames
import io.airbyte.commons.protocol.CatalogDiffHelpers.getCatalogDiff
import io.airbyte.commons.protocol.CatalogDiffHelpers.getFullyQualifiedFieldNamesWithTypes
import io.airbyte.commons.protocol.transformmodels.FieldTransform.Companion.createAddFieldTransform
import io.airbyte.commons.protocol.transformmodels.FieldTransform.Companion.createRemoveFieldTransform
import io.airbyte.commons.protocol.transformmodels.FieldTransform.Companion.createUpdateFieldTransform
import io.airbyte.commons.protocol.transformmodels.StreamAttributeTransform.Companion.createUpdatePrimaryKeyTransform
import io.airbyte.commons.protocol.transformmodels.StreamTransform
import io.airbyte.commons.protocol.transformmodels.StreamTransform.Companion.createAddStreamTransform
import io.airbyte.commons.protocol.transformmodels.StreamTransform.Companion.createRemoveStreamTransform
import io.airbyte.commons.protocol.transformmodels.StreamTransform.Companion.createUpdateStreamTransform
import io.airbyte.commons.protocol.transformmodels.StreamTransformType
import io.airbyte.commons.protocol.transformmodels.UpdateFieldSchemaTransform
import io.airbyte.commons.protocol.transformmodels.UpdateStreamTransform
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.DestinationSyncMode
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.SyncMode
import io.airbyte.config.helpers.ProtocolConverters.Companion.toProtocol
import io.airbyte.protocol.models.Jsons
import io.airbyte.protocol.models.v0.AirbyteCatalog
import io.airbyte.protocol.models.v0.AirbyteStream
import org.assertj.core.api.Condition
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.util.stream.Stream

internal class CatalogDiffHelpersTest {
  @Throws(IOException::class)
  private fun readResource(name: String): String {
    val resource = Resources.getResource(name)
    return Resources.toString(resource, StandardCharsets.UTF_8)
  }

  @Test
  @Throws(IOException::class)
  fun testGetFieldNames() {
    val node = Jsons.deserialize(readResource(VALID_SCHEMA_JSON))
    val actualFieldNames = getAllFieldNames(node)
    val expectedFieldNames =
      listOf(
        "id",
        CAD,
        "DKK",
        "HKD",
        "HUF",
        "ISK",
        "PHP",
        DATE,
        "nestedkey",
        "somekey",
        "something",
        "something2",
        "文",
        SOME_ARRAY,
        ITEMS,
        "oldName",
      )

    // sort so that the diff is easier to read.
    Assertions.assertEquals(expectedFieldNames.stream().sorted().toList(), actualFieldNames.stream().sorted().toList())
  }

  @Test
  @Throws(IOException::class)
  fun testGetCatalogDiff() {
    val schema1 = Jsons.deserialize(readResource(VALID_SCHEMA_JSON))
    val schema2 = Jsons.deserialize(readResource("diffs/valid_schema2.json"))
    val catalog1 =
      AirbyteCatalog().withStreams(
        listOf(
          AirbyteStream().withName(USERS).withJsonSchema(schema1),
          AirbyteStream().withName("accounts").withJsonSchema(Jsons.emptyObject()),
        ),
      )
    val catalog2 =
      AirbyteCatalog().withStreams(
        listOf(
          AirbyteStream().withName(USERS).withJsonSchema(schema2),
          AirbyteStream().withName(SALES).withJsonSchema(Jsons.emptyObject()),
        ),
      )

    val configuredAirbyteCatalog =
      ConfiguredAirbyteCatalog().withStreams(
        listOf(
          ConfiguredAirbyteStream(
            io.airbyte.config.AirbyteStream(USERS, schema2, listOf(SyncMode.FULL_REFRESH)),
            SyncMode.FULL_REFRESH,
            DestinationSyncMode.APPEND,
          ),
          ConfiguredAirbyteStream(
            io.airbyte.config.AirbyteStream(SALES, Jsons.emptyObject(), listOf(SyncMode.FULL_REFRESH)),
            SyncMode.FULL_REFRESH,
            DestinationSyncMode.APPEND,
          ),
        ),
      )

    val actualDiff = getCatalogDiff(catalog1, catalog2, configuredAirbyteCatalog)
    val expectedDiff =
      Stream
        .of(
          createAddStreamTransform(StreamDescriptor().withName(SALES)),
          createRemoveStreamTransform(StreamDescriptor().withName("accounts")),
          createUpdateStreamTransform(
            StreamDescriptor().withName(USERS),
            UpdateStreamTransform(
              setOf(
                createAddFieldTransform(
                  listOf("COD"),
                  schema2[PROPERTIES]["COD"],
                ),
                createAddFieldTransform(
                  listOf("somePreviouslyInvalidField"),
                  schema2[PROPERTIES]["somePreviouslyInvalidField"],
                ),
                createRemoveFieldTransform(
                  listOf("something2"),
                  schema1[PROPERTIES]["something2"],
                  false,
                ),
                createRemoveFieldTransform(
                  listOf("HKD"),
                  schema1[PROPERTIES]["HKD"],
                  false,
                ),
                createUpdateFieldTransform(
                  listOf(CAD),
                  UpdateFieldSchemaTransform(
                    schema1[PROPERTIES][CAD],
                    schema2[PROPERTIES][CAD],
                  ),
                ),
                createUpdateFieldTransform(
                  listOf(SOME_ARRAY),
                  UpdateFieldSchemaTransform(
                    schema1[PROPERTIES][SOME_ARRAY],
                    schema2[PROPERTIES][SOME_ARRAY],
                  ),
                ),
                createUpdateFieldTransform(
                  listOf(SOME_ARRAY, ITEMS),
                  UpdateFieldSchemaTransform(
                    schema1[PROPERTIES][SOME_ARRAY][ITEMS],
                    schema2[PROPERTIES][SOME_ARRAY][ITEMS],
                  ),
                ),
                createRemoveFieldTransform(
                  listOf(SOME_ARRAY, ITEMS, "oldName"),
                  schema1[PROPERTIES][SOME_ARRAY][ITEMS][PROPERTIES]["oldName"],
                  false,
                ),
                createAddFieldTransform(
                  listOf(SOME_ARRAY, ITEMS, "newName"),
                  schema2[PROPERTIES][SOME_ARRAY][ITEMS][PROPERTIES]["newName"],
                ),
              ),
              setOf(),
            ),
          ),
        ).sorted(STREAM_TRANSFORM_COMPARATOR)
        .toList()

    org.assertj.core.api.Assertions
      .assertThat(actualDiff)
      .containsAll(expectedDiff)
  }

  @Test
  @Throws(IOException::class)
  fun testGetFullyQualifiedFieldNamesWithTypes() {
    getFullyQualifiedFieldNamesWithTypes(
      Jsons.deserialize(readResource(COMPANIES_VALID)),
    ).asSequence()
      .map { pair -> mutableMapOf<List<String>, JsonNode>().also { CatalogDiffHelpers.collectInHashMap(it, pair) } }
      .reduce { acc, next ->
        CatalogDiffHelpers.combineAccumulator(acc, next)
        acc
      }
  }

  @Test
  @Throws(IOException::class)
  fun testGetFullyQualifiedFieldNamesWithTypesOnInvalidSchema() {
    val resultField =
      CatalogDiffHelpers
        .getFullyQualifiedFieldNamesWithTypes(Jsons.deserialize(readResource(COMPANIES_INVALID)))
        .stream()
        .collect(
          { HashMap() },
          CatalogDiffHelpers::collectInHashMap,
          CatalogDiffHelpers::combineAccumulator,
        )

    Assertions.assertTrue(resultField.containsKey(listOf("tags", "tags", "items")))
    Assertions.assertEquals(CatalogDiffHelpers.DUPLICATED_SCHEMA, resultField.get(listOf("tags", "tags", "items")))
  }

  @Test
  @Throws(IOException::class)
  fun testGetCatalogDiffWithInvalidSchema() {
    val schema1 = Jsons.deserialize(readResource(COMPANIES_INVALID))
    val schema2 = Jsons.deserialize(readResource(COMPANIES_VALID))
    val catalog1 =
      AirbyteCatalog().withStreams(
        listOf(
          AirbyteStream().withName(USERS).withJsonSchema(schema1),
        ),
      )
    val catalog2 =
      AirbyteCatalog().withStreams(
        listOf(
          AirbyteStream().withName(USERS).withJsonSchema(schema2),
        ),
      )

    val configuredAirbyteCatalog =
      ConfiguredAirbyteCatalog().withStreams(
        listOf(
          ConfiguredAirbyteStream(
            io.airbyte.config.AirbyteStream(USERS, schema2, listOf(SyncMode.FULL_REFRESH)),
            SyncMode.FULL_REFRESH,
            DestinationSyncMode.APPEND,
          ),
          ConfiguredAirbyteStream(
            io.airbyte.config.AirbyteStream(SALES, Jsons.emptyObject(), listOf(SyncMode.FULL_REFRESH)),
            SyncMode.FULL_REFRESH,
            DestinationSyncMode.APPEND,
          ),
        ),
      )

    val actualDiff = getCatalogDiff(catalog1, catalog2, configuredAirbyteCatalog)

    org.assertj.core.api.Assertions
      .assertThat(actualDiff)
      .hasSize(1)
    org.assertj.core.api.Assertions
      .assertThat(actualDiff)
      .first()
      .has(
        Condition(
          { streamTransform: StreamTransform -> streamTransform.transformType == StreamTransformType.UPDATE_STREAM },
          "Check update",
        ),
      )
  }

  @Test
  @Throws(IOException::class)
  fun testGetCatalogDiffWithBothInvalidSchema() {
    val schema1 = Jsons.deserialize(readResource(COMPANIES_INVALID))
    val schema2 = Jsons.deserialize(readResource(COMPANIES_INVALID))
    val catalog1 =
      AirbyteCatalog().withStreams(
        listOf(
          AirbyteStream().withName(USERS).withJsonSchema(schema1),
        ),
      )
    val catalog2 =
      AirbyteCatalog().withStreams(
        listOf(
          AirbyteStream().withName(USERS).withJsonSchema(schema2),
        ),
      )

    val configuredAirbyteCatalog =
      ConfiguredAirbyteCatalog().withStreams(
        listOf(
          ConfiguredAirbyteStream(
            io.airbyte.config.AirbyteStream(USERS, schema2, listOf(SyncMode.FULL_REFRESH)),
            SyncMode.FULL_REFRESH,
            DestinationSyncMode.APPEND,
          ),
          ConfiguredAirbyteStream(
            io.airbyte.config.AirbyteStream(SALES, Jsons.emptyObject(), listOf(SyncMode.FULL_REFRESH)),
            SyncMode.FULL_REFRESH,
            DestinationSyncMode.APPEND,
          ),
        ),
      )

    val actualDiff = getCatalogDiff(catalog1, catalog2, configuredAirbyteCatalog)

    org.assertj.core.api.Assertions
      .assertThat(actualDiff)
      .hasSize(0)
  }

  @Test
  @Throws(IOException::class)
  fun testCatalogDiffWithBreakingChanges() {
    val schema1 = Jsons.deserialize(readResource(VALID_SCHEMA_JSON))
    val breakingSchema = Jsons.deserialize(readResource("diffs/breaking_change_schema.json"))
    val catalog1 =
      AirbyteCatalog().withStreams(
        listOf(
          AirbyteStream().withName(USERS).withJsonSchema(schema1),
        ),
      )
    val catalog2 =
      AirbyteCatalog().withStreams(
        listOf(
          AirbyteStream().withName(USERS).withJsonSchema(breakingSchema),
        ),
      )

    val configuredAirbyteCatalog =
      ConfiguredAirbyteCatalog().withStreams(
        listOf(
          ConfiguredAirbyteStream(
            io.airbyte.config.AirbyteStream(USERS, schema1, listOf(SyncMode.INCREMENTAL)),
            SyncMode.INCREMENTAL,
            DestinationSyncMode.APPEND_DEDUP,
          ).withCursorField(listOf(DATE))
            .withPrimaryKey(listOf(listOf("id"))),
        ),
      )

    val diff = getCatalogDiff(catalog1, catalog2, configuredAirbyteCatalog)

    val expectedDiff =
      Stream
        .of(
          createUpdateStreamTransform(
            StreamDescriptor().withName(USERS),
            UpdateStreamTransform(
              setOf(
                createRemoveFieldTransform(
                  listOf(DATE),
                  schema1[PROPERTIES][DATE],
                  true,
                ),
                createRemoveFieldTransform(
                  listOf("id"),
                  schema1[PROPERTIES]["id"],
                  true,
                ),
              ),
              setOf(),
            ),
          ),
        ).toList()

    org.assertj.core.api.Assertions
      .assertThat(diff)
      .containsAll(expectedDiff)
  }

  @Test
  @Throws(IOException::class)
  fun testCatalogDiffWithoutStreamConfig() {
    val schema1 = Jsons.deserialize(readResource(VALID_SCHEMA_JSON))
    val breakingSchema = Jsons.deserialize(readResource("diffs/breaking_change_schema.json"))
    val catalog1 =
      AirbyteCatalog().withStreams(
        listOf(
          AirbyteStream().withName(USERS).withJsonSchema(schema1),
        ),
      )
    val catalog2 =
      AirbyteCatalog().withStreams(
        listOf(
          AirbyteStream().withName(USERS).withJsonSchema(breakingSchema),
        ),
      )

    val configuredAirbyteCatalog =
      ConfiguredAirbyteCatalog().withStreams(
        listOf(
          ConfiguredAirbyteStream(
            io.airbyte.config.AirbyteStream(SALES, schema1, listOf(SyncMode.INCREMENTAL)),
            SyncMode.INCREMENTAL,
            DestinationSyncMode.APPEND_DEDUP,
          ).withCursorField(listOf(DATE))
            .withPrimaryKey(listOf(listOf("id"))),
        ),
      )

    val diff = getCatalogDiff(catalog1, catalog2, configuredAirbyteCatalog)

    // configuredCatalog is for a different stream, so no diff should be found
    org.assertj.core.api.Assertions
      .assertThat(diff)
      .hasSize(0)
  }

  @Test
  @Throws(IOException::class)
  fun testCatalogDiffStreamChangeWithNoTransforms() {
    val schema1 = Jsons.deserialize(readResource(VALID_SCHEMA_JSON))

    val catalog1 =
      AirbyteCatalog().withStreams(
        listOf(
          io.airbyte.config
            .AirbyteStream(USERS, schema1, listOf(SyncMode.FULL_REFRESH))
            .withSourceDefinedPrimaryKey(listOf(listOf("id")))
            .toProtocol(),
          io.airbyte.config
            .AirbyteStream(SALES, schema1, listOf(SyncMode.FULL_REFRESH))
            .toProtocol(),
        ),
      )

    val catalog2 =
      AirbyteCatalog().withStreams(
        listOf(
          io.airbyte.config
            .AirbyteStream(USERS, schema1, listOf(SyncMode.FULL_REFRESH))
            .withSourceDefinedPrimaryKey(listOf(listOf("id")))
            .toProtocol(),
        ),
      )

    val configuredAirbyteCatalog =
      ConfiguredAirbyteCatalog().withStreams(
        listOf(
          ConfiguredAirbyteStream(
            io.airbyte.config.AirbyteStream(USERS, schema1, listOf(SyncMode.FULL_REFRESH)),
            SyncMode.FULL_REFRESH,
            DestinationSyncMode.APPEND,
          ),
          ConfiguredAirbyteStream(
            io.airbyte.config.AirbyteStream(SALES, schema1, listOf(SyncMode.FULL_REFRESH)),
            SyncMode.FULL_REFRESH,
            DestinationSyncMode.APPEND,
          ),
        ),
      )

    val actualDiff = getCatalogDiff(catalog1, catalog2, configuredAirbyteCatalog)

    val expectedDiff =
      Stream
        .of(
          createRemoveStreamTransform(StreamDescriptor().withName(SALES)),
        ).toList()

    org.assertj.core.api.Assertions
      .assertThat(actualDiff)
      .containsExactlyElementsOf(expectedDiff)
  }

  @ParameterizedTest
  @MethodSource("testCatalogDiffWithSourceDefinedPrimaryKeyChangeMethodSource")
  @Throws(
    IOException::class,
  )
  fun testCatalogDiffWithSourceDefinedPrimaryKeyChange(
    destSyncMode: DestinationSyncMode,
    configuredPK: List<List<String>>,
    prevSourcePK: List<List<String>>,
    newSourcePK: List<List<String>>,
    isBreaking: Boolean,
  ) {
    val schema = Jsons.deserialize(readResource(VALID_SCHEMA_JSON))

    val stream =
      io.airbyte.config
        .AirbyteStream(USERS, schema, listOf(SyncMode.FULL_REFRESH))
        .withSourceDefinedPrimaryKey(prevSourcePK)
    val refreshedStream =
      io.airbyte.config
        .AirbyteStream(USERS, schema, listOf(SyncMode.FULL_REFRESH))
        .withSourceDefinedPrimaryKey(newSourcePK)

    val initialCatalog = AirbyteCatalog().withStreams(listOf(stream.toProtocol()))

    val refreshedCatalog = AirbyteCatalog().withStreams(listOf(refreshedStream.toProtocol()))

    val configuredStream =
      ConfiguredAirbyteStream(stream, SyncMode.INCREMENTAL, destSyncMode)
        .withPrimaryKey(configuredPK)

    val configuredCatalog = ConfiguredAirbyteCatalog().withStreams(listOf(configuredStream))

    val actualDiff = getCatalogDiff(initialCatalog, refreshedCatalog, configuredCatalog)

    val expectedDiff =
      listOf(
        createUpdateStreamTransform(
          StreamDescriptor().withName(USERS),
          UpdateStreamTransform(
            setOf(),
            setOf(createUpdatePrimaryKeyTransform(prevSourcePK, newSourcePK, isBreaking)),
          ),
        ),
      )

    org.assertj.core.api.Assertions
      .assertThat(actualDiff)
      .containsExactlyElementsOf(expectedDiff)
  }

  companion object {
    // handy for debugging test only.
    private val STREAM_TRANSFORM_COMPARATOR: Comparator<StreamTransform> = Comparator.comparing { c: StreamTransform -> c.transformType }
    private const val CAD = "CAD"
    private const val ITEMS = "items"
    private const val SOME_ARRAY = "someArray"
    private const val PROPERTIES = "properties"
    private const val USERS = "users"
    private const val DATE = "date"
    private const val SALES = "sales"
    private val ID_PK: List<List<String>> = listOf(listOf("id"))
    private val DATE_PK: List<List<String>> = listOf(listOf(DATE))
    private val COMPOSITE_PK: List<List<String>> = listOf(listOf("id"), listOf(DATE))
    private const val COMPANIES_VALID = "diffs/companies_schema.json"
    private const val COMPANIES_INVALID = "diffs/companies_schema_invalid.json"
    private const val VALID_SCHEMA_JSON = "diffs/valid_schema.json"

    @JvmStatic
    private fun testCatalogDiffWithSourceDefinedPrimaryKeyChangeMethodSource(): Stream<Arguments> =
      Stream.of( // Should be breaking in DE-DUP mode if the previous PK is not the new source-defined PK
        Arguments.of(DestinationSyncMode.APPEND_DEDUP, ID_PK, ID_PK, DATE_PK, true),
        Arguments.of(DestinationSyncMode.APPEND_DEDUP, ID_PK, listOf<Any>(), DATE_PK, true),
        Arguments.of(DestinationSyncMode.APPEND_DEDUP, COMPOSITE_PK, COMPOSITE_PK, ID_PK, true),
        Arguments.of(DestinationSyncMode.OVERWRITE_DEDUP, ID_PK, ID_PK, DATE_PK, true),
        Arguments.of(DestinationSyncMode.OVERWRITE_DEDUP, ID_PK, listOf<Any>(), DATE_PK, true),
        Arguments.of(
          DestinationSyncMode.OVERWRITE_DEDUP,
          COMPOSITE_PK,
          COMPOSITE_PK,
          ID_PK,
          true,
        ), // Should not be breaking in DE-DUP mode if the previous and new source-defined PK contain the
        // fields in a different order
        Arguments.of(DestinationSyncMode.APPEND_DEDUP, COMPOSITE_PK, COMPOSITE_PK, COMPOSITE_PK.reversed(), false),
        Arguments.of(
          DestinationSyncMode.OVERWRITE_DEDUP,
          COMPOSITE_PK,
          COMPOSITE_PK,
          COMPOSITE_PK.reversed(),
          false,
        ), // Should not be breaking in other sync modes
        Arguments.of(DestinationSyncMode.APPEND, ID_PK, ID_PK, DATE_PK, false),
        Arguments.of(DestinationSyncMode.OVERWRITE, ID_PK, ID_PK, DATE_PK, false),
        Arguments.of(DestinationSyncMode.APPEND, ID_PK, listOf<Any>(), DATE_PK, false),
        Arguments.of(DestinationSyncMode.OVERWRITE, ID_PK, listOf<Any>(), DATE_PK, false),
        Arguments.of(DestinationSyncMode.APPEND, COMPOSITE_PK, COMPOSITE_PK, ID_PK, false),
        Arguments.of(
          DestinationSyncMode.OVERWRITE,
          COMPOSITE_PK,
          COMPOSITE_PK,
          ID_PK,
          false,
        ), // Should not be breaking if added source-defined PK is already the manually set PK
        Arguments.of(DestinationSyncMode.APPEND, ID_PK, listOf<Any>(), ID_PK, false),
        Arguments.of(DestinationSyncMode.APPEND_DEDUP, ID_PK, listOf<Any>(), ID_PK, false),
        Arguments.of(DestinationSyncMode.OVERWRITE, ID_PK, listOf<Any>(), ID_PK, false),
        Arguments.of(
          DestinationSyncMode.OVERWRITE_DEDUP,
          ID_PK,
          listOf<Any>(),
          ID_PK,
          false,
        ), // Removing source-defined PK should not be breaking
        Arguments.of(DestinationSyncMode.APPEND, ID_PK, ID_PK, listOf<Any>(), false),
        Arguments.of(DestinationSyncMode.APPEND_DEDUP, ID_PK, ID_PK, listOf<Any>(), false),
        Arguments.of(DestinationSyncMode.OVERWRITE, ID_PK, ID_PK, listOf<Any>(), false),
        Arguments.of(DestinationSyncMode.OVERWRITE_DEDUP, ID_PK, ID_PK, listOf<Any>(), false),
      )
  }
}
