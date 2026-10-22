/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters

import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.api.server.generated.models.AirbyteCatalog
import io.airbyte.api.server.generated.models.AirbyteStreamAndConfiguration
import io.airbyte.api.server.generated.models.AirbyteStreamConfiguration
import io.airbyte.api.server.generated.models.ConfiguredStreamMapper
import io.airbyte.api.server.generated.models.DestinationSyncMode
import io.airbyte.api.server.generated.models.SelectedFieldInfo
import io.airbyte.api.server.generated.models.StreamMapperType
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
import io.micronaut.context.annotation.Property
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.util.UUID

@MicronautTest
@Property(name = "INTERNAL_API_HOST", value = "http://localhost:8080")
internal class CatalogConverterTest {
  @Inject
  lateinit var catalogConverter: CatalogConverter

  @Test
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
    Assertions.assertTrue(isCompatible<io.airbyte.config.SyncMode, io.airbyte.api.server.generated.models.SyncMode>())
    Assertions.assertTrue(isCompatible<io.airbyte.config.DestinationSyncMode, DestinationSyncMode>())
    Assertions.assertTrue(isCompatible<io.airbyte.config.DataType, io.airbyte.api.model.generated.DataType>())
  }

  @Test
  fun testConvertInternal() {
    val hashingMapper = createHashingMapper(ConnectionHelpers.SECOND_FIELD_NAME)
    val hashingMapper2 = createHashingMapper(ConnectionHelpers.FIELD_NAME, UUID.randomUUID())
    val apiCatalog =
      ConnectionHelpers.generateApiCatalogWithTwoFields().withFirstConfig {
        it.copy(
          mappers =
            listOf(
              ConfiguredStreamMapper(
                type = StreamMapperType.HASHING,
                mapperConfiguration = jsonNode<HashingConfig?>(hashingMapper.config),
              ),
              ConfiguredStreamMapper(
                id = hashingMapper2.id(),
                type = StreamMapperType.HASHING,
                mapperConfiguration = jsonNode<HashingConfig?>(hashingMapper2.config),
              ),
            ),
        )
      }

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
  fun testConvertInternalWithFiles(includeFiles: Boolean) {
    val apiCatalog = ConnectionHelpers.generateApiCatalogWithTwoFields().withFirstConfig { it.copy(includeFiles = includeFiles) }

    val internalCatalog = catalogConverter.toConfiguredInternal(apiCatalog)
    Assertions.assertEquals(1, internalCatalog.streams.size)
    val internalStream: ConfiguredAirbyteStream = internalCatalog.streams.first()
    Assertions.assertEquals(includeFiles, internalStream.includeFiles)
  }

  @Test
  fun testConvertInternalWithDestinationObjectName() {
    val destinationObjectName = "test_destination_object_name"
    val apiCatalog =
      ConnectionHelpers.generateApiCatalogWithTwoFields().withFirstConfig { it.copy(destinationObjectName = destinationObjectName) }

    val internalCatalog = catalogConverter.toConfiguredInternal(apiCatalog)
    Assertions.assertEquals(1, internalCatalog.streams.size)
    val internalStream: ConfiguredAirbyteStream = internalCatalog.streams.first()
    Assertions.assertEquals(destinationObjectName, internalStream.destinationObjectName)
  }

  @Test
  fun testConvertInternalWithHashedFields() {
    val apiCatalog =
      ConnectionHelpers.generateApiCatalogWithTwoFields().withFirstConfig {
        it.copy(hashedFields = listOf(SelectedFieldInfo(fieldPath = listOf(ConnectionHelpers.SECOND_FIELD_NAME))))
      }

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
  fun testConvertToInternalOnlyIncludeSelectedFields() {
    val twoFieldsCatalog = ConnectionHelpers.generateApiCatalogWithTwoFields()
    val apiStream: AirbyteStreamAndConfiguration = twoFieldsCatalog.streams.first()
    Assertions.assertEquals(
      2,
      apiStream
        .stream!!
        .jsonSchema!!
        .get("properties")
        .size(),
    )

    val apiCatalog = twoFieldsCatalog.withFirstConfig { it.selectFields(ConnectionHelpers.FIELD_NAME) }

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
      val catalog = ConnectionHelpers.generateBasicApiCatalog().withFirstConfig { it.copy(fieldSelectionEnabled = true, selectedFields = null) }
      catalogConverter.toConfiguredInternal(catalog)
    }

    Assertions.assertThrows(JsonValidationException::class.java) {
      // JSON schema has no `properties` node.
      val basicCatalog = ConnectionHelpers.generateBasicApiCatalog()
      (basicCatalog.streams[0].stream!!.jsonSchema as ObjectNode).remove("properties")
      val catalog = basicCatalog.withFirstConfig { it.selectFields("foo") }
      catalogConverter.toConfiguredInternal(catalog)
    }

    Assertions.assertThrows(JsonValidationException::class.java) {
      // SelectedFieldInfo with empty path.
      val catalog =
        ConnectionHelpers.generateBasicApiCatalog().withFirstConfig {
          it.copy(fieldSelectionEnabled = true, selectedFields = it.selectedFields.orEmpty() + SelectedFieldInfo())
        }
      catalogConverter.toConfiguredInternal(catalog)
    }

    Assertions.assertThrows(UnsupportedOperationException::class.java) {
      // SelectedFieldInfo with nested field path.
      val catalog =
        ConnectionHelpers.generateBasicApiCatalog().withFirstConfig {
          it.copy(fieldSelectionEnabled = true, selectedFields = it.selectedFields.orEmpty() + SelectedFieldInfo(fieldPath = listOf("foo", "bar")))
        }
      catalogConverter.toConfiguredInternal(catalog)
    }

    Assertions.assertThrows(JsonValidationException::class.java) {
      // SelectedFieldInfo with empty path.
      val catalog = ConnectionHelpers.generateBasicApiCatalog().withFirstConfig { it.selectFields("foo") }
      catalogConverter.toConfiguredInternal(catalog)
    }

    Assertions.assertThrows(JsonValidationException::class.java) {
      // Only FIELD_NAME is selected.
      // The sync mode is INCREMENTAL and SECOND_FIELD_NAME is a cursor field.
      val catalog =
        ConnectionHelpers.generateApiCatalogWithTwoFields().withFirstConfig {
          it
            .selectFields(
              ConnectionHelpers.FIELD_NAME,
            ).copy(syncMode = io.airbyte.api.server.generated.models.SyncMode.INCREMENTAL, cursorField = listOf(ConnectionHelpers.SECOND_FIELD_NAME))
        }
      catalogConverter.toConfiguredInternal(catalog)
    }

    Assertions.assertDoesNotThrow {
      // Only FIELD_NAME is selected.
      // The cursor field is not selected, but it's okay because it's FULL_REFRESH so it doesn't throw.
      val catalog =
        ConnectionHelpers.generateApiCatalogWithTwoFields().withFirstConfig {
          it
            .selectFields(
              ConnectionHelpers.FIELD_NAME,
            ).copy(syncMode = io.airbyte.api.server.generated.models.SyncMode.FULL_REFRESH, cursorField = listOf(ConnectionHelpers.SECOND_FIELD_NAME))
        }
      catalogConverter.toConfiguredInternal(catalog)
    }

    Assertions.assertThrows(JsonValidationException::class.java) {
      // Only FIELD_NAME is selected.
      // The destination sync mode is DEDUP and SECOND_FIELD_NAME is a primary key.
      val catalog =
        ConnectionHelpers.generateApiCatalogWithTwoFields().withFirstConfig {
          it
            .selectFields(
              ConnectionHelpers.FIELD_NAME,
            ).copy(destinationSyncMode = DestinationSyncMode.APPEND_DEDUP, primaryKey = listOf(listOf(ConnectionHelpers.SECOND_FIELD_NAME)))
        }
      catalogConverter.toConfiguredInternal(catalog)
    }

    Assertions.assertDoesNotThrow {
      // Only FIELD_NAME is selected.
      // The primary key is not selected but that's okay because the destination sync mode is OVERWRITE.
      val catalog =
        ConnectionHelpers.generateApiCatalogWithTwoFields().withFirstConfig {
          it
            .selectFields(
              ConnectionHelpers.FIELD_NAME,
            ).copy(destinationSyncMode = DestinationSyncMode.OVERWRITE, primaryKey = listOf(listOf(ConnectionHelpers.SECOND_FIELD_NAME)))
        }
      catalogConverter.toConfiguredInternal(catalog)
    }
  }

  @Test
  fun testConvertToProtocolFieldSelection() {
    val catalog = ConnectionHelpers.generateApiCatalogWithTwoFields().withFirstConfig { it.selectFields(ConnectionHelpers.FIELD_NAME) }
    Assertions.assertEquals(ConnectionHelpers.generateBasicConfiguredAirbyteCatalog(), catalogConverter.toConfiguredInternal(catalog))
  }

  @Test
  fun testDiscoveredToApiDefaultSyncModesNoSourceCursor() {
    val persistedCatalog = catalogConverter.toProtocol(ConnectionHelpers.generateBasicApiCatalog())
    val actualStreamConfig =
      catalogConverter
        .toApi(persistedCatalog, null)
        .streams[0]
        .config!!
    val actualSyncMode = actualStreamConfig.syncMode
    val actualDestinationSyncMode = actualStreamConfig.destinationSyncMode
    Assertions.assertEquals(io.airbyte.api.server.generated.models.SyncMode.FULL_REFRESH, actualSyncMode)
    Assertions.assertEquals(DestinationSyncMode.OVERWRITE, actualDestinationSyncMode)
  }

  @Test
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
        .streams[0]
        .config!!
    val actualSyncMode = actualStreamConfig.syncMode
    val actualDestinationSyncMode = actualStreamConfig.destinationSyncMode
    Assertions.assertEquals(io.airbyte.api.server.generated.models.SyncMode.INCREMENTAL, actualSyncMode)
    Assertions.assertEquals(DestinationSyncMode.APPEND_DEDUP, actualDestinationSyncMode)
  }

  @Test
  fun testDiscoveredToApiDefaultSyncModesSourceCursorNoPrimaryKey() {
    val persistedCatalog = catalogConverter.toProtocol(ConnectionHelpers.generateBasicApiCatalog())
    persistedCatalog.getStreams().get(0).withSourceDefinedCursor(true)
    val actualStreamConfig =
      catalogConverter
        .toApi(persistedCatalog, null)
        .streams[0]
        .config!!
    val actualSyncMode = actualStreamConfig.syncMode
    val actualDestinationSyncMode = actualStreamConfig.destinationSyncMode
    Assertions.assertEquals(io.airbyte.api.server.generated.models.SyncMode.FULL_REFRESH, actualSyncMode)
    Assertions.assertEquals(DestinationSyncMode.OVERWRITE, actualDestinationSyncMode)
  }

  @Test
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
        .streams[0]
        .config!!
    val actualSyncMode = actualStreamConfig.syncMode
    val actualDestinationSyncMode = actualStreamConfig.destinationSyncMode
    Assertions.assertEquals(io.airbyte.api.server.generated.models.SyncMode.INCREMENTAL, actualSyncMode)
    Assertions.assertEquals(DestinationSyncMode.APPEND, actualDestinationSyncMode)
  }

  private fun AirbyteCatalog.withFirstConfig(config: (AirbyteStreamConfiguration) -> AirbyteStreamConfiguration): AirbyteCatalog {
    val first = streams[0]
    return copy(streams = listOf(first.copy(config = config(first.config!!))) + streams.drop(1))
  }

  private fun AirbyteStreamConfiguration.selectFields(vararg fieldNames: String): AirbyteStreamConfiguration =
    copy(
      fieldSelectionEnabled = true,
      selectedFields = selectedFields.orEmpty() + fieldNames.map { SelectedFieldInfo(fieldPath = listOf(it)) },
    )
}
