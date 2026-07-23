/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import io.airbyte.api.model.generated.AirbyteCatalog
import io.airbyte.api.model.generated.AirbyteStream
import io.airbyte.api.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.model.generated.DestinationSyncMode
import io.airbyte.api.model.generated.SelectedFieldInfo
import io.airbyte.api.model.generated.SyncMode
import io.airbyte.commons.server.helpers.ConnectionHelpers.SECOND_FIELD_NAME
import io.airbyte.commons.server.helpers.ConnectionHelpers.generateApiCatalogWithTwoFields
import io.airbyte.commons.server.helpers.ConnectionHelpers.generateBasicApiCatalog
import io.airbyte.config.helpers.FieldGenerator
import io.airbyte.protocol.models.JsonSchemaType
import io.airbyte.protocol.models.v0.CatalogHelpers
import io.airbyte.protocol.models.v0.Field
import jakarta.validation.Valid
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
    val original = AirbyteCatalog().streams(mutableListOf<@Valid AirbyteStreamAndConfiguration?>())
    val discovered = generateBasicApiCatalog()
    discovered
      .getStreams()
      .get(0)
      .getStream()
      .name(STREAM1)
      .jsonSchema(CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD1, JsonSchemaType.STRING)))
      .supportedSyncModes(listOf<SyncMode?>(SyncMode.FULL_REFRESH))
    discovered
      .getStreams()
      .get(0)
      .getConfig()
      .syncMode(SyncMode.FULL_REFRESH)
      .cursorField(mutableListOf<String?>())
      .destinationSyncMode(DestinationSyncMode.OVERWRITE)
      .primaryKey(mutableListOf<MutableList<String?>?>())
      .aliasName(STREAM1)

    val expected = generateBasicApiCatalog()
    expected
      .getStreams()
      .get(0)
      .getStream()
      .name(STREAM1)
      .jsonSchema(CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD1, JsonSchemaType.STRING)))
      .supportedSyncModes(listOf<SyncMode?>(SyncMode.FULL_REFRESH))
    expected
      .getStreams()
      .get(0)
      .getConfig()
      .syncMode(SyncMode.FULL_REFRESH)
      .cursorField(mutableListOf<String?>())
      .destinationSyncMode(DestinationSyncMode.OVERWRITE)
      .primaryKey(mutableListOf<MutableList<String?>?>())
      .aliasName(STREAM1)
      .selected(false)
      .suggested(false)
      .selectedFields(mutableListOf<@Valid SelectedFieldInfo?>())

    val actual = catalogMergeHelper.mergeCatalogWithConfiguration(original, original, discovered)

    Assertions.assertEquals(expected, actual)
  }

  @Test
  fun testUpdateSchemaWithDiscoveryResetStream() {
    val original = generateBasicApiCatalog()
    original
      .getStreams()
      .get(0)
      .getStream()
      .name("random-stream")
      .defaultCursorField(listOf<String?>(FIELD1))
      .jsonSchema(
        CatalogHelpers.fieldsToJsonSchema(
          Field.of(FIELD1, JsonSchemaType.NUMBER),
          Field.of(FIELD2, JsonSchemaType.NUMBER),
          Field.of(FIELD5, JsonSchemaType.STRING),
        ),
      ).supportedSyncModes(listOf<SyncMode?>(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))
    original
      .getStreams()
      .get(0)
      .getConfig()
      .syncMode(SyncMode.INCREMENTAL)
      .cursorField(listOf<String?>(FIELD1))
      .destinationSyncMode(DestinationSyncMode.APPEND)
      .primaryKey(mutableListOf<MutableList<String?>?>())
      .aliasName("random_stream")

    val discovered = generateBasicApiCatalog()
    discovered
      .getStreams()
      .get(0)
      .getStream()
      .name(STREAM1)
      .defaultCursorField(listOf<String?>(FIELD3))
      .jsonSchema(CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD2, JsonSchemaType.STRING)))
      .supportedSyncModes(listOf<SyncMode?>(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))
    discovered
      .getStreams()
      .get(0)
      .getConfig()
      .syncMode(SyncMode.FULL_REFRESH)
      .cursorField(mutableListOf<String?>())
      .destinationSyncMode(DestinationSyncMode.OVERWRITE)
      .primaryKey(mutableListOf<MutableList<String?>?>())
      .aliasName(STREAM1)

    val expected = generateBasicApiCatalog()
    expected
      .getStreams()
      .get(0)
      .getStream()
      .name(STREAM1)
      .defaultCursorField(listOf<String?>(FIELD3))
      .jsonSchema(CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD2, JsonSchemaType.STRING)))
      .supportedSyncModes(listOf<SyncMode?>(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))
    expected
      .getStreams()
      .get(0)
      .getConfig()
      .syncMode(SyncMode.FULL_REFRESH)
      .cursorField(mutableListOf<String?>())
      .destinationSyncMode(DestinationSyncMode.OVERWRITE)
      .primaryKey(mutableListOf<MutableList<String?>?>())
      .aliasName(STREAM1)
      .selected(false)
      .suggested(false)
      .selectedFields(mutableListOf<@Valid SelectedFieldInfo?>())

    val actual = catalogMergeHelper.mergeCatalogWithConfiguration(original, original, discovered)

    Assertions.assertEquals(expected, actual)
  }

  @Test
  fun testUpdateSchemaWithDiscoveryMergeNewStream() {
    val original = generateBasicApiCatalog()
    original
      .getStreams()
      .get(0)
      .getStream()
      .name(STREAM1)
      .defaultCursorField(listOf<String?>(FIELD1))
      .jsonSchema(
        CatalogHelpers.fieldsToJsonSchema(
          Field.of(FIELD1, JsonSchemaType.NUMBER),
          Field.of(FIELD2, JsonSchemaType.NUMBER),
          Field.of(FIELD5, JsonSchemaType.STRING),
        ),
      ).supportedSyncModes(listOf<SyncMode?>(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))
    original
      .getStreams()
      .get(0)
      .getConfig()
      .syncMode(SyncMode.INCREMENTAL)
      .cursorField(listOf<String?>(FIELD1))
      .destinationSyncMode(DestinationSyncMode.APPEND)
      .primaryKey(mutableListOf<MutableList<String?>?>())
      .aliasName("renamed_stream")

    val discovered = generateBasicApiCatalog()
    discovered
      .getStreams()
      .get(0)
      .getStream()
      .name(STREAM1)
      .defaultCursorField(listOf<String?>(FIELD3))
      .jsonSchema(CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD2, JsonSchemaType.STRING)))
      .supportedSyncModes(listOf<SyncMode?>(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))
    discovered
      .getStreams()
      .get(0)
      .getConfig()
      .syncMode(SyncMode.FULL_REFRESH)
      .cursorField(mutableListOf<String?>())
      .destinationSyncMode(DestinationSyncMode.OVERWRITE)
      .primaryKey(mutableListOf<MutableList<String?>?>())
      .aliasName(STREAM1)
    val newStream = generateBasicApiCatalog().getStreams().get(0)
    newStream
      .getStream()
      .name(STREAM2)
      .defaultCursorField(listOf<String?>(FIELD5))
      .jsonSchema(CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD5, JsonSchemaType.BOOLEAN)))
      .supportedSyncModes(listOf<SyncMode?>(SyncMode.FULL_REFRESH))
    newStream
      .getConfig()
      .syncMode(SyncMode.FULL_REFRESH)
      .cursorField(mutableListOf<String?>())
      .destinationSyncMode(DestinationSyncMode.OVERWRITE)
      .primaryKey(mutableListOf<MutableList<String?>?>())
      .aliasName(STREAM2)
    discovered.getStreams().add(newStream)

    val expected = generateBasicApiCatalog()
    expected
      .getStreams()
      .get(0)
      .getStream()
      .name(STREAM1)
      .defaultCursorField(listOf<String?>(FIELD3))
      .jsonSchema(CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD2, JsonSchemaType.STRING)))
      .supportedSyncModes(listOf<SyncMode?>(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))
    expected
      .getStreams()
      .get(0)
      .getConfig()
      .syncMode(SyncMode.INCREMENTAL)
      .cursorField(listOf<String?>(FIELD1))
      .destinationSyncMode(DestinationSyncMode.APPEND)
      .primaryKey(mutableListOf<MutableList<String?>?>())
      .aliasName("renamed_stream")
      .selected(true)
      .suggested(false)
      .selectedFields(mutableListOf<@Valid SelectedFieldInfo?>())
    val expectedNewStream = generateBasicApiCatalog().getStreams().get(0)
    expectedNewStream
      .getStream()
      .name(STREAM2)
      .defaultCursorField(listOf<String?>(FIELD5))
      .jsonSchema(CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD5, JsonSchemaType.BOOLEAN)))
      .supportedSyncModes(listOf<SyncMode?>(SyncMode.FULL_REFRESH))
    expectedNewStream
      .getConfig()
      .syncMode(SyncMode.FULL_REFRESH)
      .cursorField(mutableListOf<String?>())
      .destinationSyncMode(DestinationSyncMode.OVERWRITE)
      .primaryKey(mutableListOf<MutableList<String?>?>())
      .aliasName(STREAM2)
      .selected(false)
      .suggested(false)
      .selectedFields(mutableListOf<@Valid SelectedFieldInfo?>())
    expected.getStreams().add(expectedNewStream)

    val actual = catalogMergeHelper.mergeCatalogWithConfiguration(original, original, discovered)

    Assertions.assertEquals(expected, actual)
  }

  @Test
  fun testUpdateSchemaWithDiscoveryWithChangedSourceDefinedPK() {
    val original = generateBasicApiCatalog()
    original
      .getStreams()
      .first()
      .getStream()
      .sourceDefinedPrimaryKey(listOf(listOf<String?>(FIELD1)))
    original
      .getStreams()
      .first()
      .getConfig()
      .primaryKey(listOf(listOf<String?>(FIELD1)))

    val discovered = generateBasicApiCatalog()
    discovered
      .getStreams()
      .first()
      .getStream()
      .sourceDefinedPrimaryKey(listOf(listOf<String?>(FIELD2)))
    discovered
      .getStreams()
      .first()
      .getConfig()
      .primaryKey(listOf(listOf<String?>(FIELD2)))

    val actual = catalogMergeHelper.mergeCatalogWithConfiguration(original, original, discovered)

    // Use new value for source-defined PK
    Assertions.assertEquals(
      listOf(listOf<String?>(FIELD2)),
      actual
        .getStreams()
        .first()
        .getConfig()
        .getPrimaryKey(),
    )
  }

  @Test
  fun testUpdateSchemaWithDiscoveryWithNoSourceDefinedPK() {
    val original = generateBasicApiCatalog()
    original
      .getStreams()
      .first()
      .getConfig()
      .primaryKey(listOf(listOf<String?>(FIELD1)))

    val discovered = generateBasicApiCatalog()
    Assertions.assertNotEquals(
      original
        .getStreams()
        .first()
        .getConfig()
        .getPrimaryKey(),
      discovered
        .getStreams()
        .first()
        .getConfig()
        .getPrimaryKey(),
    )

    val actual = catalogMergeHelper.mergeCatalogWithConfiguration(original, original, discovered)

    // Keep previously-configured PK
    Assertions.assertEquals(
      listOf(listOf<String?>(FIELD1)),
      actual
        .getStreams()
        .first()
        .getConfig()
        .getPrimaryKey(),
    )
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun testUpdateSchemaWithDiscoveryWithIncludeFiles(includeFiles: Boolean) {
    val original = generateBasicApiCatalog()
    original
      .getStreams()
      .first()
      .getConfig()
      .includeFiles(includeFiles)

    val discovered = generateBasicApiCatalog()
    discovered
      .getStreams()
      .first()
      .getConfig()
      .includeFiles(false)

    val actual = catalogMergeHelper.mergeCatalogWithConfiguration(original, original, discovered)

    // Use new value for include files
    Assertions.assertEquals(
      includeFiles,
      actual
        .getStreams()
        .first()
        .getConfig()
        .getIncludeFiles(),
    )
  }

  @Test
  fun testUpdateSchemaWithDestinationObjectName() {
    val configured = generateBasicApiCatalog()
    configured
      .getStreams()
      .first()
      .getConfig()
      .destinationObjectName("configured_object_name")

    val discovered = generateBasicApiCatalog()

    val actual = catalogMergeHelper.mergeCatalogWithConfiguration(configured, discovered, discovered)

    Assertions.assertEquals(
      "configured_object_name",
      actual
        .getStreams()
        .first()
        .getConfig()
        .getDestinationObjectName(),
    )
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun testUpdateSchemaWithDiscoveryWithFileBased(isFileBased: Boolean) {
    val original = generateBasicApiCatalog()
    original
      .getStreams()
      .first()
      .getStream()
      .isFileBased(false)

    val discovered = generateBasicApiCatalog()
    discovered
      .getStreams()
      .first()
      .getStream()
      .isFileBased(isFileBased)

    val actual = catalogMergeHelper.mergeCatalogWithConfiguration(original, original, discovered)

    Assertions.assertEquals(
      isFileBased,
      actual
        .getStreams()
        .first()
        .getStream()
        .getIsFileBased(),
    )
  }

  @Test
  fun testUpdateSchemaWithDiscoveryWithHashedField() {
    val hashedFields = listOf<SelectedFieldInfo?>(SelectedFieldInfo().fieldPath(listOf<String?>(SECOND_FIELD_NAME)))

    val original = generateApiCatalogWithTwoFields()
    original
      .getStreams()
      .first()
      .getConfig()
      .setHashedFields(hashedFields)

    val discovered = generateApiCatalogWithTwoFields()

    val actual = catalogMergeHelper.mergeCatalogWithConfiguration(original, original, discovered)

    // Configure hashed fields
    Assertions.assertEquals(
      hashedFields,
      actual
        .getStreams()
        .first()
        .getConfig()
        .getHashedFields(),
    )
  }

  @Test
  fun testUpdateSchemaWithDiscoveryWithRemovedHashedField() {
    val original = generateApiCatalogWithTwoFields()
    original
      .getStreams()
      .first()
      .getConfig()
      .setHashedFields(listOf<@Valid SelectedFieldInfo?>(SelectedFieldInfo().fieldPath(listOf<String?>(SECOND_FIELD_NAME))))

    val discovered = generateBasicApiCatalog()

    val actual = catalogMergeHelper.mergeCatalogWithConfiguration(original, original, discovered)

    // Remove hashed field
    Assertions.assertTrue(
      actual
        .getStreams()
        .first()
        .getConfig()
        .getHashedFields()
        .isEmpty(),
    )
  }

  @Test
  fun testUpdateSchemaWithNamespacedStreams() {
    val original = generateBasicApiCatalog()
    val stream1Config = original.getStreams().get(0)
    val stream1 = stream1Config.getStream()
    val stream2 =
      AirbyteStream()
        .name(stream1.getName())
        .namespace("second_namespace")
        .jsonSchema(stream1.getJsonSchema())
        .defaultCursorField(stream1.getDefaultCursorField())
        .supportedSyncModes(stream1.getSupportedSyncModes())
        .sourceDefinedCursor(stream1.getSourceDefinedCursor())
        .sourceDefinedPrimaryKey(stream1.getSourceDefinedPrimaryKey())
    val stream2Config =
      AirbyteStreamAndConfiguration()
        .config(stream1Config.getConfig())
        .stream(stream2)
    original.getStreams().add(stream2Config)

    val discovered = generateBasicApiCatalog()
    discovered
      .getStreams()
      .get(0)
      .getStream()
      .name(STREAM1)
      .jsonSchema(CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD1, JsonSchemaType.STRING)))
      .supportedSyncModes(listOf<SyncMode?>(SyncMode.FULL_REFRESH))
    discovered
      .getStreams()
      .get(0)
      .getConfig()
      .syncMode(SyncMode.FULL_REFRESH)
      .cursorField(mutableListOf<String?>())
      .destinationSyncMode(DestinationSyncMode.OVERWRITE)
      .primaryKey(mutableListOf<MutableList<String?>?>())
      .aliasName(STREAM1)

    val expected = generateBasicApiCatalog()
    expected
      .getStreams()
      .get(0)
      .getStream()
      .name(STREAM1)
      .jsonSchema(CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD1, JsonSchemaType.STRING)))
      .supportedSyncModes(listOf<SyncMode?>(SyncMode.FULL_REFRESH))
    expected
      .getStreams()
      .get(0)
      .getConfig()
      .syncMode(SyncMode.FULL_REFRESH)
      .cursorField(mutableListOf<String?>())
      .destinationSyncMode(DestinationSyncMode.OVERWRITE)
      .primaryKey(mutableListOf<MutableList<String?>?>())
      .aliasName(STREAM1)
      .selected(false)
      .suggested(false)
      .selectedFields(mutableListOf<@Valid SelectedFieldInfo?>())

    val actual = catalogMergeHelper.mergeCatalogWithConfiguration(original, original, discovered)

    Assertions.assertEquals(expected, actual)
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
