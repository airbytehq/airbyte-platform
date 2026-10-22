/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import io.airbyte.api.server.generated.models.AirbyteCatalog
import io.airbyte.api.server.generated.models.AirbyteStream
import io.airbyte.api.server.generated.models.AirbyteStreamAndConfiguration
import io.airbyte.api.server.generated.models.AirbyteStreamConfiguration
import io.airbyte.api.server.generated.models.DestinationSyncMode
import io.airbyte.api.server.generated.models.SelectedFieldInfo
import io.airbyte.api.server.generated.models.SyncMode
import io.airbyte.commons.server.helpers.ConnectionHelpers.SECOND_FIELD_NAME
import io.airbyte.commons.server.helpers.ConnectionHelpers.generateApiCatalogWithTwoFields
import io.airbyte.commons.server.helpers.ConnectionHelpers.generateBasicApiCatalog
import io.airbyte.config.helpers.FieldGenerator
import io.airbyte.protocol.models.JsonSchemaType
import io.airbyte.protocol.models.v0.CatalogHelpers
import io.airbyte.protocol.models.v0.Field
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

internal class CatalogMergeHelperTest {
  private lateinit var catalogMergeHelper: CatalogMergeHelper
  private val fieldGenerator = FieldGenerator()

  @BeforeEach
  fun setup() {
    catalogMergeHelper = CatalogMergeHelper(fieldGenerator)
  }

  @Test
  fun testUpdateSchemaWithDiscoveryFromEmpty() {
    val original = AirbyteCatalog(streams = emptyList())
    val discovered =
      generateBasicApiCatalog().withFirstStream(
        stream = {
          it.copy(
            name = STREAM1,
            jsonSchema = CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD1, JsonSchemaType.STRING)),
            supportedSyncModes = listOf(SyncMode.FULL_REFRESH),
          )
        },
        config = {
          it.copy(
            syncMode = SyncMode.FULL_REFRESH,
            cursorField = emptyList(),
            destinationSyncMode = DestinationSyncMode.OVERWRITE,
            primaryKey = emptyList(),
            aliasName = STREAM1,
          )
        },
      )

    val expected =
      generateBasicApiCatalog().withFirstStream(
        stream = {
          it.copy(
            name = STREAM1,
            jsonSchema = CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD1, JsonSchemaType.STRING)),
            supportedSyncModes = listOf(SyncMode.FULL_REFRESH),
          )
        },
        config = {
          it.copy(
            syncMode = SyncMode.FULL_REFRESH,
            cursorField = emptyList(),
            destinationSyncMode = DestinationSyncMode.OVERWRITE,
            primaryKey = emptyList(),
            aliasName = STREAM1,
            selected = false,
            suggested = false,
            selectedFields = emptyList(),
          )
        },
      )

    val actual = catalogMergeHelper.mergeCatalogWithConfiguration(original, original, discovered)

    Assertions.assertEquals(expected, actual)
  }

  @Test
  fun testUpdateSchemaWithDiscoveryResetStream() {
    val original =
      generateBasicApiCatalog().withFirstStream(
        stream = {
          it.copy(
            name = "random-stream",
            defaultCursorField = listOf(FIELD1),
            jsonSchema =
              CatalogHelpers.fieldsToJsonSchema(
                Field.of(FIELD1, JsonSchemaType.NUMBER),
                Field.of(FIELD2, JsonSchemaType.NUMBER),
                Field.of(FIELD5, JsonSchemaType.STRING),
              ),
            supportedSyncModes = listOf(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL),
          )
        },
        config = {
          it.copy(
            syncMode = SyncMode.INCREMENTAL,
            cursorField = listOf(FIELD1),
            destinationSyncMode = DestinationSyncMode.APPEND,
            primaryKey = emptyList(),
            aliasName = "random_stream",
          )
        },
      )

    val discovered =
      generateBasicApiCatalog().withFirstStream(
        stream = {
          it.copy(
            name = STREAM1,
            defaultCursorField = listOf(FIELD3),
            jsonSchema = CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD2, JsonSchemaType.STRING)),
            supportedSyncModes = listOf(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL),
          )
        },
        config = {
          it.copy(
            syncMode = SyncMode.FULL_REFRESH,
            cursorField = emptyList(),
            destinationSyncMode = DestinationSyncMode.OVERWRITE,
            primaryKey = emptyList(),
            aliasName = STREAM1,
          )
        },
      )

    val expected =
      generateBasicApiCatalog().withFirstStream(
        stream = {
          it.copy(
            name = STREAM1,
            defaultCursorField = listOf(FIELD3),
            jsonSchema = CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD2, JsonSchemaType.STRING)),
            supportedSyncModes = listOf(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL),
          )
        },
        config = {
          it.copy(
            syncMode = SyncMode.FULL_REFRESH,
            cursorField = emptyList(),
            destinationSyncMode = DestinationSyncMode.OVERWRITE,
            primaryKey = emptyList(),
            aliasName = STREAM1,
            selected = false,
            suggested = false,
            selectedFields = emptyList(),
          )
        },
      )

    val actual = catalogMergeHelper.mergeCatalogWithConfiguration(original, original, discovered)

    Assertions.assertEquals(expected, actual)
  }

  @Test
  fun testUpdateSchemaWithDiscoveryMergeNewStream() {
    val original =
      generateBasicApiCatalog().withFirstStream(
        stream = {
          it.copy(
            name = STREAM1,
            defaultCursorField = listOf(FIELD1),
            jsonSchema =
              CatalogHelpers.fieldsToJsonSchema(
                Field.of(FIELD1, JsonSchemaType.NUMBER),
                Field.of(FIELD2, JsonSchemaType.NUMBER),
                Field.of(FIELD5, JsonSchemaType.STRING),
              ),
            supportedSyncModes = listOf(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL),
          )
        },
        config = {
          it.copy(
            syncMode = SyncMode.INCREMENTAL,
            cursorField = listOf(FIELD1),
            destinationSyncMode = DestinationSyncMode.APPEND,
            primaryKey = emptyList(),
            aliasName = "renamed_stream",
          )
        },
      )

    val newStream =
      generateBasicApiCatalog()
        .withFirstStream(
          stream = {
            it.copy(
              name = STREAM2,
              defaultCursorField = listOf(FIELD5),
              jsonSchema = CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD5, JsonSchemaType.BOOLEAN)),
              supportedSyncModes = listOf(SyncMode.FULL_REFRESH),
            )
          },
          config = {
            it.copy(
              syncMode = SyncMode.FULL_REFRESH,
              cursorField = emptyList(),
              destinationSyncMode = DestinationSyncMode.OVERWRITE,
              primaryKey = emptyList(),
              aliasName = STREAM2,
            )
          },
        ).streams[0]
    val discoveredBase =
      generateBasicApiCatalog().withFirstStream(
        stream = {
          it.copy(
            name = STREAM1,
            defaultCursorField = listOf(FIELD3),
            jsonSchema = CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD2, JsonSchemaType.STRING)),
            supportedSyncModes = listOf(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL),
          )
        },
        config = {
          it.copy(
            syncMode = SyncMode.FULL_REFRESH,
            cursorField = emptyList(),
            destinationSyncMode = DestinationSyncMode.OVERWRITE,
            primaryKey = emptyList(),
            aliasName = STREAM1,
          )
        },
      )
    val discovered = discoveredBase.copy(streams = discoveredBase.streams + newStream)

    val expectedNewStream =
      newStream.copy(
        config =
          newStream.config!!.copy(
            selected = false,
            suggested = false,
            selectedFields = emptyList(),
          ),
      )
    val expectedBase =
      generateBasicApiCatalog().withFirstStream(
        stream = {
          it.copy(
            name = STREAM1,
            defaultCursorField = listOf(FIELD3),
            jsonSchema = CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD2, JsonSchemaType.STRING)),
            supportedSyncModes = listOf(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL),
          )
        },
        config = {
          it.copy(
            syncMode = SyncMode.INCREMENTAL,
            cursorField = listOf(FIELD1),
            destinationSyncMode = DestinationSyncMode.APPEND,
            primaryKey = emptyList(),
            aliasName = "renamed_stream",
            selected = true,
            suggested = false,
            selectedFields = emptyList(),
          )
        },
      )
    val expected = expectedBase.copy(streams = expectedBase.streams + expectedNewStream)

    val actual = catalogMergeHelper.mergeCatalogWithConfiguration(original, original, discovered)

    Assertions.assertEquals(expected, actual)
  }

  @Test
  fun testUpdateSchemaWithDiscoveryWithChangedSourceDefinedPK() {
    val original =
      generateBasicApiCatalog().withFirstStream(
        stream = { it.copy(sourceDefinedPrimaryKey = listOf(listOf(FIELD1))) },
        config = { it.copy(primaryKey = listOf(listOf(FIELD1))) },
      )

    val discovered =
      generateBasicApiCatalog().withFirstStream(
        stream = { it.copy(sourceDefinedPrimaryKey = listOf(listOf(FIELD2))) },
        config = { it.copy(primaryKey = listOf(listOf(FIELD2))) },
      )

    val actual = catalogMergeHelper.mergeCatalogWithConfiguration(original, original, discovered)

    // Use new value for source-defined PK
    Assertions.assertEquals(
      listOf(listOf(FIELD2)),
      actual.streams
        .first()
        .config!!
        .primaryKey,
    )
  }

  @Test
  fun testUpdateSchemaWithDiscoveryWithNoSourceDefinedPK() {
    val original =
      generateBasicApiCatalog().withFirstStream(
        config = { it.copy(primaryKey = listOf(listOf(FIELD1))) },
      )

    val discovered = generateBasicApiCatalog()
    Assertions.assertNotEquals(
      original.streams
        .first()
        .config!!
        .primaryKey,
      discovered.streams
        .first()
        .config!!
        .primaryKey,
    )

    val actual = catalogMergeHelper.mergeCatalogWithConfiguration(original, original, discovered)

    // Keep previously-configured PK
    Assertions.assertEquals(
      listOf(listOf(FIELD1)),
      actual.streams
        .first()
        .config!!
        .primaryKey,
    )
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun testUpdateSchemaWithDiscoveryWithIncludeFiles(includeFiles: Boolean) {
    val original = generateBasicApiCatalog().withFirstStream(config = { it.copy(includeFiles = includeFiles) })

    val discovered = generateBasicApiCatalog().withFirstStream(config = { it.copy(includeFiles = false) })

    val actual = catalogMergeHelper.mergeCatalogWithConfiguration(original, original, discovered)

    // Use new value for include files
    Assertions.assertEquals(
      includeFiles,
      actual.streams
        .first()
        .config!!
        .includeFiles,
    )
  }

  @Test
  fun testUpdateSchemaWithDestinationObjectName() {
    val configured =
      generateBasicApiCatalog().withFirstStream(config = { it.copy(destinationObjectName = "configured_object_name") })

    val discovered = generateBasicApiCatalog()

    val actual = catalogMergeHelper.mergeCatalogWithConfiguration(configured, discovered, discovered)

    Assertions.assertEquals(
      "configured_object_name",
      actual.streams
        .first()
        .config!!
        .destinationObjectName,
    )
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun testUpdateSchemaWithDiscoveryWithFileBased(isFileBased: Boolean) {
    val original = generateBasicApiCatalog().withFirstStream(stream = { it.copy(isFileBased = false) })

    val discovered = generateBasicApiCatalog().withFirstStream(stream = { it.copy(isFileBased = isFileBased) })

    val actual = catalogMergeHelper.mergeCatalogWithConfiguration(original, original, discovered)

    Assertions.assertEquals(
      isFileBased,
      actual.streams
        .first()
        .stream!!
        .isFileBased,
    )
  }

  @Test
  fun testUpdateSchemaWithDiscoveryWithHashedField() {
    val hashedFields = listOf(SelectedFieldInfo(fieldPath = listOf(SECOND_FIELD_NAME)))

    val original = generateApiCatalogWithTwoFields().withFirstStream(config = { it.copy(hashedFields = hashedFields) })

    val discovered = generateApiCatalogWithTwoFields()

    val actual = catalogMergeHelper.mergeCatalogWithConfiguration(original, original, discovered)

    // Configure hashed fields
    Assertions.assertEquals(
      hashedFields,
      actual.streams
        .first()
        .config!!
        .hashedFields,
    )
  }

  @Test
  fun testUpdateSchemaWithDiscoveryWithRemovedHashedField() {
    val original =
      generateApiCatalogWithTwoFields().withFirstStream(
        config = { it.copy(hashedFields = listOf(SelectedFieldInfo(fieldPath = listOf(SECOND_FIELD_NAME)))) },
      )

    val discovered = generateBasicApiCatalog()

    val actual = catalogMergeHelper.mergeCatalogWithConfiguration(original, original, discovered)

    // Remove hashed field
    Assertions.assertTrue(
      actual.streams
        .first()
        .config!!
        .hashedFields!!
        .isEmpty(),
    )
  }

  @Test
  fun testUpdateSchemaWithNamespacedStreams() {
    val basicCatalog = generateBasicApiCatalog()
    val stream1Config = basicCatalog.streams[0]
    val stream1 = stream1Config.stream!!
    val stream2 =
      AirbyteStream(
        name = stream1.name,
        namespace = "second_namespace",
        jsonSchema = stream1.jsonSchema,
        defaultCursorField = stream1.defaultCursorField,
        supportedSyncModes = stream1.supportedSyncModes,
        sourceDefinedCursor = stream1.sourceDefinedCursor,
        sourceDefinedPrimaryKey = stream1.sourceDefinedPrimaryKey,
      )
    val stream2Config =
      AirbyteStreamAndConfiguration(config = stream1Config.config, stream = stream2)
    val original = basicCatalog.copy(streams = basicCatalog.streams + stream2Config)

    val discovered =
      generateBasicApiCatalog().withFirstStream(
        stream = {
          it.copy(
            name = STREAM1,
            jsonSchema = CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD1, JsonSchemaType.STRING)),
            supportedSyncModes = listOf(SyncMode.FULL_REFRESH),
          )
        },
        config = {
          it.copy(
            syncMode = SyncMode.FULL_REFRESH,
            cursorField = emptyList(),
            destinationSyncMode = DestinationSyncMode.OVERWRITE,
            primaryKey = emptyList(),
            aliasName = STREAM1,
          )
        },
      )

    val expected =
      generateBasicApiCatalog().withFirstStream(
        stream = {
          it.copy(
            name = STREAM1,
            jsonSchema = CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD1, JsonSchemaType.STRING)),
            supportedSyncModes = listOf(SyncMode.FULL_REFRESH),
          )
        },
        config = {
          it.copy(
            syncMode = SyncMode.FULL_REFRESH,
            cursorField = emptyList(),
            destinationSyncMode = DestinationSyncMode.OVERWRITE,
            primaryKey = emptyList(),
            aliasName = STREAM1,
            selected = false,
            suggested = false,
            selectedFields = emptyList(),
          )
        },
      )

    val actual = catalogMergeHelper.mergeCatalogWithConfiguration(original, original, discovered)

    Assertions.assertEquals(expected, actual)
  }

  private fun AirbyteCatalog.withFirstStream(
    stream: (AirbyteStream) -> AirbyteStream = { it },
    config: (AirbyteStreamConfiguration) -> AirbyteStreamConfiguration = { it },
  ): AirbyteCatalog {
    val first = streams[0]
    return copy(streams = listOf(first.copy(stream = stream(first.stream!!), config = config(first.config!!))) + streams.drop(1))
  }

  companion object {
    private const val STREAM1 = "stream1"
    private const val STREAM2 = "stream2"
    private const val FIELD1 = "field1"
    private const val FIELD2 = "field2"
    private const val FIELD3 = "field3"
    private const val FIELD5 = "field5"
  }
}
