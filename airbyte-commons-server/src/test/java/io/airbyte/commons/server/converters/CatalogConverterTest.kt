/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters

import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.api.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.model.generated.ConfiguredStreamMapper
import io.airbyte.api.model.generated.DestinationSyncMode
import io.airbyte.api.model.generated.SelectedFieldInfo
import io.airbyte.api.model.generated.StreamMapperType
import io.airbyte.commons.enums.isCompatible
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.commons.server.handlers.helpers.CatalogConverter
import io.airbyte.commons.server.helpers.ConnectionHelpers
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.Field
import io.airbyte.config.FieldSelectionData
import io.airbyte.config.FieldType
import io.airbyte.config.MapperConfig
import io.airbyte.config.SyncMode
import io.airbyte.config.mapper.configs.HashingConfig
import io.airbyte.mappers.helpers.createHashingMapper
import io.airbyte.validation.json.JsonValidationException
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.util.UUID

@MicronautTest
internal class CatalogConverterTest {
  @Inject
  lateinit var catalogConverter: CatalogConverter

  @Test
  @Throws(JsonValidationException::class)
  fun testConvertToProtocol() {
    Assertions.assertEquals(
      ConnectionHelpers.generateBasicConfiguredAirbyteCatalog(),
      catalogConverter.toConfiguredInternal(ConnectionHelpers.generateBasicApiCatalog()),
    )
  }

  @Test
  fun testConvertToAPI() {
    Assertions.assertEquals(
      ConnectionHelpers.generateBasicApiCatalog(),
      catalogConverter.toApi(
        ConnectionHelpers.generateBasicConfiguredAirbyteCatalog(),
        FieldSelectionData(),
      ),
    )
  }

  @Test
  fun testEnumCompatibility() {
    Assertions.assertTrue(isCompatible<io.airbyte.config.SyncMode, io.airbyte.api.model.generated.SyncMode>())
    Assertions.assertTrue(isCompatible<io.airbyte.config.DestinationSyncMode, DestinationSyncMode>())
    Assertions.assertTrue(isCompatible<io.airbyte.config.DataType, io.airbyte.api.model.generated.DataType>())
  }

  @Test
  @Throws(JsonValidationException::class)
  fun testConvertInternal() {
    val hashingMapper = createHashingMapper(ConnectionHelpers.SECOND_FIELD_NAME)
    val hashingMapper2 = createHashingMapper(ConnectionHelpers.FIELD_NAME, UUID.randomUUID())
    val apiCatalog = ConnectionHelpers.generateApiCatalogWithTwoFields()
    val apiStream: AirbyteStreamAndConfiguration = apiCatalog.getStreams().first()
    apiStream.getConfig().setMappers(
      listOf(
        ConfiguredStreamMapper()
          .type(StreamMapperType.HASHING)
          .mapperConfiguration(jsonNode<HashingConfig?>(hashingMapper.config)),
        ConfiguredStreamMapper()
          .id(hashingMapper2.id())
          .type(StreamMapperType.HASHING)
          .mapperConfiguration(jsonNode<HashingConfig?>(hashingMapper2.config)),
      ),
    )

    val internalCatalog = catalogConverter.toConfiguredInternal(apiCatalog)
    Assertions.assertEquals(1, internalCatalog.streams.size)
    val internalStream: ConfiguredAirbyteStream = internalCatalog.streams.first()
    val mappers: List<MapperConfig> = internalStream.mappers
    Assertions.assertEquals(2, mappers.size)

    val fields: List<Field>? = internalStream.fields
    Assertions.assertEquals(2, fields!!.size)

    Assertions.assertEquals(hashingMapper, mappers.first())
    Assertions.assertEquals(hashingMapper2, mappers.get(1))
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  @Throws(JsonValidationException::class)
  fun testConvertInternalWithFiles(includeFiles: Boolean) {
    val apiCatalog = ConnectionHelpers.generateApiCatalogWithTwoFields()
    val apiStream: AirbyteStreamAndConfiguration = apiCatalog.getStreams().first()
    apiStream.getConfig().setIncludeFiles(includeFiles)

    val internalCatalog = catalogConverter.toConfiguredInternal(apiCatalog)
    Assertions.assertEquals(1, internalCatalog.streams.size)
    val internalStream: ConfiguredAirbyteStream = internalCatalog.streams.first()
    Assertions.assertEquals(includeFiles, internalStream.includeFiles)
  }

  @Test
  @Throws(JsonValidationException::class)
  fun testConvertInternalWithDestinationObjectName() {
    val apiCatalog = ConnectionHelpers.generateApiCatalogWithTwoFields()
    val apiStream: AirbyteStreamAndConfiguration = apiCatalog.getStreams().first()
    val destinationObjectName = "test_destination_object_name"
    apiStream.getConfig().setDestinationObjectName(destinationObjectName)

    val internalCatalog = catalogConverter.toConfiguredInternal(apiCatalog)
    Assertions.assertEquals(1, internalCatalog.streams.size)
    val internalStream: ConfiguredAirbyteStream = internalCatalog.streams.first()
    Assertions.assertEquals(destinationObjectName, internalStream.destinationObjectName)
  }

  @Test
  @Throws(JsonValidationException::class)
  fun testConvertInternalWithHashedFields() {
    val apiCatalog = ConnectionHelpers.generateApiCatalogWithTwoFields()
    val apiStream: AirbyteStreamAndConfiguration = apiCatalog.getStreams().first()
    apiStream
      .getConfig()
      .setHashedFields(listOf(SelectedFieldInfo().fieldPath(listOf(ConnectionHelpers.SECOND_FIELD_NAME))))

    val internalCatalog = catalogConverter.toConfiguredInternal(apiCatalog)
    Assertions.assertEquals(1, internalCatalog.streams.size)
    val internalStream: ConfiguredAirbyteStream = internalCatalog.streams.first()
    val mappers: List<MapperConfig?> = internalStream.mappers
    Assertions.assertEquals(1, mappers.size)

    val fields: List<Field?>? = internalStream.fields
    Assertions.assertEquals(2, fields!!.size)

    val expectedMapper = createHashingMapper(ConnectionHelpers.SECOND_FIELD_NAME)
    Assertions.assertEquals(expectedMapper, mappers.first())
  }

  @Test
  @Throws(JsonValidationException::class)
  fun testConvertToInternalOnlyIncludeSelectedFields() {
    val apiCatalog = ConnectionHelpers.generateApiCatalogWithTwoFields()
    val apiStream: AirbyteStreamAndConfiguration = apiCatalog.getStreams().first()
    Assertions.assertEquals(
      2,
      apiStream
        .getStream()
        .getJsonSchema()
        .get("properties")
        .size(),
    )

    apiStream.getConfig().fieldSelectionEnabled(true).addSelectedFieldsItem(SelectedFieldInfo().addFieldPathItem(ConnectionHelpers.FIELD_NAME))

    val internalCatalog = catalogConverter.toConfiguredInternal(apiCatalog)
    Assertions.assertEquals(1, internalCatalog.streams.size)

    val internalStream: ConfiguredAirbyteStream = internalCatalog.streams.first()
    val properties = internalStream.stream.jsonSchema.get("properties") as ObjectNode
    Assertions.assertEquals(1, properties.size())
    Assertions.assertTrue(properties.has(ConnectionHelpers.FIELD_NAME))

    val fields: List<Field?>? = internalStream.fields
    Assertions.assertNotNull(fields)
    Assertions.assertEquals(1, fields!!.size)

    val expectedField = Field(ConnectionHelpers.FIELD_NAME, FieldType.STRING, false)
    Assertions.assertEquals(expectedField, fields.first())
  }

  @Test
  fun testConvertToProtocolColumnSelectionValidation() {
    Assertions.assertThrows(JsonValidationException::class.java) {
      // fieldSelectionEnabled=true but selectedFields=null.
      val catalog = ConnectionHelpers.generateBasicApiCatalog()
      catalog
        .getStreams()
        .get(0)
        .getConfig()
        .fieldSelectionEnabled(true)
        .selectedFields(null)
      catalogConverter.toConfiguredInternal(catalog)
    }

    Assertions.assertThrows(JsonValidationException::class.java) {
      // JSON schema has no `properties` node.
      val catalog = ConnectionHelpers.generateBasicApiCatalog()
      (
        catalog
          .getStreams()
          .get(0)
          .getStream()
          .getJsonSchema() as ObjectNode
      ).remove("properties")
      catalog
        .getStreams()
        .get(0)
        .getConfig()
        .fieldSelectionEnabled(true)
        .addSelectedFieldsItem(SelectedFieldInfo().addFieldPathItem("foo"))
      catalogConverter.toConfiguredInternal(catalog)
    }

    Assertions.assertThrows(JsonValidationException::class.java) {
      // SelectedFieldInfo with empty path.
      val catalog = ConnectionHelpers.generateBasicApiCatalog()
      catalog
        .getStreams()
        .get(0)
        .getConfig()
        .fieldSelectionEnabled(true)
        .addSelectedFieldsItem(SelectedFieldInfo())
      catalogConverter.toConfiguredInternal(catalog)
    }

    Assertions.assertThrows(UnsupportedOperationException::class.java) {
      // SelectedFieldInfo with nested field path.
      val catalog = ConnectionHelpers.generateBasicApiCatalog()
      catalog
        .getStreams()
        .get(0)
        .getConfig()
        .fieldSelectionEnabled(true)
        .addSelectedFieldsItem(SelectedFieldInfo().addFieldPathItem("foo").addFieldPathItem("bar"))
      catalogConverter.toConfiguredInternal(catalog)
    }

    Assertions.assertThrows(JsonValidationException::class.java) {
      // SelectedFieldInfo with empty path.
      val catalog = ConnectionHelpers.generateBasicApiCatalog()
      catalog
        .getStreams()
        .get(0)
        .getConfig()
        .fieldSelectionEnabled(true)
        .addSelectedFieldsItem(SelectedFieldInfo().addFieldPathItem("foo"))
      catalogConverter.toConfiguredInternal(catalog)
    }

    Assertions.assertThrows(JsonValidationException::class.java) {
      val catalog = ConnectionHelpers.generateApiCatalogWithTwoFields()
      // Only FIELD_NAME is selected.
      catalog
        .getStreams()
        .get(0)
        .getConfig()
        .fieldSelectionEnabled(true)
        .addSelectedFieldsItem(SelectedFieldInfo().addFieldPathItem(ConnectionHelpers.FIELD_NAME))
      // The sync mode is INCREMENTAL and SECOND_FIELD_NAME is a cursor field.
      catalog
        .getStreams()
        .get(0)
        .getConfig()
        .syncMode(io.airbyte.api.model.generated.SyncMode.INCREMENTAL)
        .cursorField(listOf(ConnectionHelpers.SECOND_FIELD_NAME))
      catalogConverter.toConfiguredInternal(catalog)
    }

    Assertions.assertDoesNotThrow {
      val catalog = ConnectionHelpers.generateApiCatalogWithTwoFields()
      // Only FIELD_NAME is selected.
      catalog
        .getStreams()
        .get(0)
        .getConfig()
        .fieldSelectionEnabled(true)
        .addSelectedFieldsItem(SelectedFieldInfo().addFieldPathItem(ConnectionHelpers.FIELD_NAME))
      // The cursor field is not selected, but it's okay because it's FULL_REFRESH so it doesn't throw.
      catalog
        .getStreams()
        .get(0)
        .getConfig()
        .syncMode(io.airbyte.api.model.generated.SyncMode.FULL_REFRESH)
        .cursorField(listOf(ConnectionHelpers.SECOND_FIELD_NAME))
      catalogConverter.toConfiguredInternal(catalog)
    }

    Assertions.assertThrows(JsonValidationException::class.java) {
      val catalog = ConnectionHelpers.generateApiCatalogWithTwoFields()
      // Only FIELD_NAME is selected.
      catalog
        .getStreams()
        .get(0)
        .getConfig()
        .fieldSelectionEnabled(true)
        .addSelectedFieldsItem(SelectedFieldInfo().addFieldPathItem(ConnectionHelpers.FIELD_NAME))
      // The destination sync mode is DEDUP and SECOND_FIELD_NAME is a primary key.
      catalog
        .getStreams()
        .get(0)
        .getConfig()
        .destinationSyncMode(DestinationSyncMode.APPEND_DEDUP)
        .primaryKey(listOf(listOf(ConnectionHelpers.SECOND_FIELD_NAME)))
      catalogConverter.toConfiguredInternal(catalog)
    }

    Assertions.assertDoesNotThrow {
      val catalog = ConnectionHelpers.generateApiCatalogWithTwoFields()
      // Only FIELD_NAME is selected.
      catalog
        .getStreams()
        .get(0)
        .getConfig()
        .fieldSelectionEnabled(true)
        .addSelectedFieldsItem(SelectedFieldInfo().addFieldPathItem(ConnectionHelpers.FIELD_NAME))
      // The primary key is not selected but that's okay because the destination sync mode is OVERWRITE.
      catalog
        .getStreams()
        .get(0)
        .getConfig()
        .destinationSyncMode(DestinationSyncMode.OVERWRITE)
        .primaryKey(listOf(listOf(ConnectionHelpers.SECOND_FIELD_NAME)))
      catalogConverter.toConfiguredInternal(catalog)
    }
  }

  @Test
  @Throws(JsonValidationException::class)
  fun testConvertToProtocolFieldSelection() {
    val catalog = ConnectionHelpers.generateApiCatalogWithTwoFields()
    catalog
      .getStreams()
      .get(0)
      .getConfig()
      .fieldSelectionEnabled(true)
      .addSelectedFieldsItem(SelectedFieldInfo().addFieldPathItem(ConnectionHelpers.FIELD_NAME))
    Assertions.assertEquals(ConnectionHelpers.generateBasicConfiguredAirbyteCatalog(), catalogConverter.toConfiguredInternal(catalog))
  }

  @Test
  @Throws(JsonValidationException::class)
  fun testDiscoveredToApiDefaultSyncModesNoSourceCursor() {
    val persistedCatalog = catalogConverter.toProtocol(ConnectionHelpers.generateBasicApiCatalog())
    val actualStreamConfig =
      catalogConverter
        .toApi(persistedCatalog, null)
        .getStreams()
        .get(0)
        .getConfig()
    val actualSyncMode = actualStreamConfig.getSyncMode()
    val actualDestinationSyncMode = actualStreamConfig.getDestinationSyncMode()
    Assertions.assertEquals(io.airbyte.api.model.generated.SyncMode.FULL_REFRESH, actualSyncMode)
    Assertions.assertEquals(DestinationSyncMode.OVERWRITE, actualDestinationSyncMode)
  }

  @Test
  @Throws(JsonValidationException::class)
  fun testDiscoveredToApiDefaultSyncModesSourceCursorAndPrimaryKey() {
    val persistedCatalog = catalogConverter.toProtocol(ConnectionHelpers.generateBasicApiCatalog())
    persistedCatalog
      .getStreams()
      .get(0)
      .withSourceDefinedCursor(true)
      .withSourceDefinedPrimaryKey(listOf(mutableListOf("unused")))
    val actualStreamConfig =
      catalogConverter
        .toApi(persistedCatalog, null)
        .getStreams()
        .get(0)
        .getConfig()
    val actualSyncMode = actualStreamConfig.getSyncMode()
    val actualDestinationSyncMode = actualStreamConfig.getDestinationSyncMode()
    Assertions.assertEquals(io.airbyte.api.model.generated.SyncMode.INCREMENTAL, actualSyncMode)
    Assertions.assertEquals(DestinationSyncMode.APPEND_DEDUP, actualDestinationSyncMode)
  }

  @Test
  @Throws(JsonValidationException::class)
  fun testDiscoveredToApiDefaultSyncModesSourceCursorNoPrimaryKey() {
    val persistedCatalog = catalogConverter.toProtocol(ConnectionHelpers.generateBasicApiCatalog())
    persistedCatalog.getStreams().get(0).withSourceDefinedCursor(true)
    val actualStreamConfig =
      catalogConverter
        .toApi(persistedCatalog, null)
        .getStreams()
        .get(0)
        .getConfig()
    val actualSyncMode = actualStreamConfig.getSyncMode()
    val actualDestinationSyncMode = actualStreamConfig.getDestinationSyncMode()
    Assertions.assertEquals(io.airbyte.api.model.generated.SyncMode.FULL_REFRESH, actualSyncMode)
    Assertions.assertEquals(DestinationSyncMode.OVERWRITE, actualDestinationSyncMode)
  }

  @Test
  @Throws(JsonValidationException::class)
  fun testDiscoveredToApiDefaultSyncModesSourceCursorNoFullRefresh() {
    val persistedCatalog = catalogConverter.toProtocol(ConnectionHelpers.generateBasicApiCatalog())
    persistedCatalog
      .getStreams()
      .get(0)
      .withSourceDefinedCursor(true)
      .withSupportedSyncModes(listOf(io.airbyte.protocol.models.v0.SyncMode.INCREMENTAL))
    val actualStreamConfig =
      catalogConverter
        .toApi(persistedCatalog, null)
        .getStreams()
        .get(0)
        .getConfig()
    val actualSyncMode = actualStreamConfig.getSyncMode()
    val actualDestinationSyncMode = actualStreamConfig.getDestinationSyncMode()
    Assertions.assertEquals(io.airbyte.api.model.generated.SyncMode.INCREMENTAL, actualSyncMode)
    Assertions.assertEquals(DestinationSyncMode.APPEND, actualDestinationSyncMode)
  }
}
