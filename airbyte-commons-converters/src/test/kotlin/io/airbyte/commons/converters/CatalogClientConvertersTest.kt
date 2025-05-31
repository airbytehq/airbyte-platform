/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.converters

import com.google.common.collect.Lists
import io.airbyte.api.client.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.client.model.generated.ConfiguredStreamMapper
import io.airbyte.api.client.model.generated.StreamMapperType
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.text.Names
import io.airbyte.config.helpers.FieldGenerator
import io.airbyte.mappers.helpers.createHashingMapper
import io.airbyte.protocol.models.JsonSchemaType
import io.airbyte.protocol.models.v0.AirbyteCatalog
import io.airbyte.protocol.models.v0.AirbyteStream
import io.airbyte.protocol.models.v0.CatalogHelpers
import io.airbyte.protocol.models.v0.Field
import io.airbyte.protocol.models.v0.SyncMode
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

const val ID_FIELD_NAME = "id"
private const val STREAM_NAME = "users-data"
private val STREAM =
  AirbyteStream()
    .withName(STREAM_NAME)
    .withJsonSchema(
      CatalogHelpers.fieldsToJsonSchema(
        Field.of(ID_FIELD_NAME, JsonSchemaType.STRING),
      ),
    ).withDefaultCursorField(listOf(ID_FIELD_NAME))
    .withSourceDefinedCursor(false)
    .withSourceDefinedPrimaryKey(emptyList())
    .withSupportedSyncModes(listOf(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))

private val CLIENT_STREAM =
  io.airbyte.api.client.model.generated.AirbyteStream(
    STREAM_NAME,
    CatalogHelpers.fieldsToJsonSchema(Field.of(ID_FIELD_NAME, JsonSchemaType.STRING)),
    listOf(
      io.airbyte.api.client.model.generated.SyncMode.FULL_REFRESH,
      io.airbyte.api.client.model.generated.SyncMode.INCREMENTAL,
    ),
    false,
    listOf(ID_FIELD_NAME),
    listOf(),
    null,
    null,
  )
private val CLIENT_DEFAULT_STREAM_CONFIGURATION =
  io.airbyte.api.client.model.generated.AirbyteStreamConfiguration(
    syncMode = io.airbyte.api.client.model.generated.SyncMode.FULL_REFRESH,
    destinationSyncMode = io.airbyte.api.client.model.generated.DestinationSyncMode.APPEND,
    cursorField = listOf(ID_FIELD_NAME),
    primaryKey = listOf(),
    aliasName = Names.toAlphanumericAndUnderscore(STREAM_NAME),
    selected = true,
    includeFiles = false,
    suggested = null,
    fieldSelectionEnabled = null,
    selectedFields = null,
    hashedFields = null,
    mappers = null,
    minimumGenerationId = null,
    generationId = null,
    syncId = null,
  )

private val BASIC_MODEL_CATALOG =
  AirbyteCatalog().withStreams(
    Lists.newArrayList(STREAM),
  )

private val EXPECTED_CLIENT_CATALOG =
  io.airbyte.api.client.model.generated.AirbyteCatalog(
    listOf(
      AirbyteStreamAndConfiguration(
        CLIENT_STREAM,
        CLIENT_DEFAULT_STREAM_CONFIGURATION,
      ),
    ),
  )

@MicronautTest
class CatalogClientConvertersTest {
  @Inject
  lateinit var fieldGenerator: FieldGenerator

  @Inject
  lateinit var catalogClientConverters: CatalogClientConverters

  @Test
  fun testConvertToClientAPI() {
    assertEquals(
      EXPECTED_CLIENT_CATALOG,
      catalogClientConverters.toAirbyteCatalogClientApi(BASIC_MODEL_CATALOG),
    )
  }

  @Test
  fun testConvertInternalWithMapping() {
    val mapperId = UUID.randomUUID()
    val hashingMapper = createHashingMapper(ID_FIELD_NAME, mapperId)

    val streamConfig =
      io.airbyte.api.client.model.generated.AirbyteStreamConfiguration(
        io.airbyte.api.client.model.generated.SyncMode.FULL_REFRESH,
        io.airbyte.api.client.model.generated.DestinationSyncMode.APPEND,
        listOf(ID_FIELD_NAME),
        null,
        listOf(),
        Names.toAlphanumericAndUnderscore(STREAM_NAME),
        true,
        null,
        null,
        null,
        null,
        null,
        listOf(
          ConfiguredStreamMapper(
            StreamMapperType.HASHING,
            Jsons.jsonNode(hashingMapper.config),
            mapperId,
          ),
        ),
        null,
        null,
        null,
      )
    val clientCatalog =
      io.airbyte.api.client.model.generated.AirbyteCatalog(
        listOf(
          AirbyteStreamAndConfiguration(
            CLIENT_STREAM,
            streamConfig,
          ),
        ),
      )

    val configuredCatalog = catalogClientConverters.toConfiguredAirbyteInternal(clientCatalog)
    val stream = configuredCatalog.streams.first()
    assertEquals(STREAM_NAME, stream.stream.name)
    assertEquals(1, stream.fields?.size)
    assertEquals(1, stream.mappers?.size)
    assertEquals(
      fieldGenerator.getFieldsFromSchema(stream.stream.jsonSchema),
      stream.fields,
    )
    assertEquals(hashingMapper, stream.mappers.first())
  }

  @Test
  fun testIsResumableImport() {
    val boolValues = mutableListOf<Boolean?>(true, false)
    boolValues.add(null)
    for (isResumable in boolValues) {
      val catalog =
        AirbyteCatalog().withStreams(
          listOf(
            AirbyteStream().withName("user").withIsResumable(isResumable),
          ),
        )
      val apiCatalog = catalogClientConverters.toAirbyteCatalogClientApi(catalog)
      assertEquals(isResumable, apiCatalog.streams[0].stream?.isResumable)
    }
  }
}
