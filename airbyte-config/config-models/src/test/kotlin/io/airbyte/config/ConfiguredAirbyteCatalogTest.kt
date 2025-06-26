/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import io.airbyte.commons.json.Jsons
import io.airbyte.config.mapper.configs.TestConfig
import io.airbyte.config.mapper.configs.TestEnums
import io.airbyte.config.mapper.configs.TestMapperConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.UUID

internal class ConfiguredAirbyteCatalogTest {
  @Test
  fun `test complete serialization and deserialization with all properties`() {
    val originalCatalog = createFullConfiguredAirbyteCatalog()

    // Serialize to JSON string
    val jsonString = Jsons.serialize(originalCatalog)
    assertNotNull(jsonString)
    assertTrue(jsonString.contains("destination_object_name"))
    assertTrue(jsonString.contains("test_destination_table"))

    // Deserialize back to object
    val deserializedCatalog = Jsons.deserialize(jsonString, ConfiguredAirbyteCatalog::class.java)

    // Assert catalog level properties
    assertEquals(originalCatalog.streams.size, deserializedCatalog.streams.size)

    // Assert stream level properties
    val originalStream = originalCatalog.streams[0]
    val deserializedStream = deserializedCatalog.streams[0]

    assertEquals(originalStream.stream.name, deserializedStream.stream.name)
    assertEquals(originalStream.stream.namespace, deserializedStream.stream.namespace)
    assertEquals(originalStream.syncMode, deserializedStream.syncMode)
    assertEquals(originalStream.destinationSyncMode, deserializedStream.destinationSyncMode)
    assertEquals(originalStream.cursorField, deserializedStream.cursorField)
    assertEquals(originalStream.primaryKey, deserializedStream.primaryKey)
    assertEquals(originalStream.generationId, deserializedStream.generationId)
    assertEquals(originalStream.minimumGenerationId, deserializedStream.minimumGenerationId)
    assertEquals(originalStream.syncId, deserializedStream.syncId)
    assertEquals(originalStream.includeFiles, deserializedStream.includeFiles)

    // destinationObjectName should be preserved
    assertEquals(originalStream.destinationObjectName, deserializedStream.destinationObjectName)
    assertEquals("test_destination_table", deserializedStream.destinationObjectName)

    // Test fields
    assertEquals(originalStream.fields?.size, deserializedStream.fields?.size)
    if (originalStream.fields != null && deserializedStream.fields != null) {
      assertEquals(originalStream.fields!![0].name, deserializedStream.fields!![0].name)
      assertEquals(originalStream.fields!![0].type, deserializedStream.fields!![0].type)
    }

    // Test mappers
    assertEquals(originalStream.mappers.size, deserializedStream.mappers.size)
    if (originalStream.mappers.isNotEmpty() && deserializedStream.mappers.isNotEmpty()) {
      val originalMapper = originalStream.mappers[0] as TestMapperConfig
      val deserializedMapper = deserializedStream.mappers[0] as TestMapperConfig
      assertEquals(originalMapper.name, deserializedMapper.name)
      assertEquals(originalMapper.config.field1, deserializedMapper.config.field1)
    }
  }

  @Test
  fun `test serialization and deserialization with minimal properties`() {
    val minimalStream =
      ConfiguredAirbyteStream(
        stream = createBasicAirbyteStream(),
        syncMode = SyncMode.FULL_REFRESH,
        destinationSyncMode = DestinationSyncMode.OVERWRITE,
      )

    val minimalCatalog = ConfiguredAirbyteCatalog(listOf(minimalStream))

    // Serialize to JSON string
    val jsonString = Jsons.serialize(minimalCatalog)

    // Deserialize back to object
    val deserializedCatalog = Jsons.deserialize(jsonString, ConfiguredAirbyteCatalog::class.java)

    // Assert basic properties are preserved
    assertEquals(1, deserializedCatalog.streams.size)
    val stream = deserializedCatalog.streams[0]
    assertEquals("test_stream", stream.stream.name)
    assertEquals(SyncMode.FULL_REFRESH, stream.syncMode)
    assertEquals(DestinationSyncMode.OVERWRITE, stream.destinationSyncMode)
  }

  @Test
  fun `test deserialization from JSON string with destinationObjectName`() {
    val jsonWithDestinationObjectName =
      """
      {
        "streams": [
          {
            "stream": {
              "name": "users",
              "json_schema": {},
              "supported_sync_modes": ["full_refresh"]
            },
            "sync_mode": "full_refresh",
            "destination_sync_mode": "overwrite",
            "destination_object_name": "custom_users_table"
          }
        ]
      }
      """.trimIndent()

    val catalog = Jsons.deserialize(jsonWithDestinationObjectName, ConfiguredAirbyteCatalog::class.java)

    assertEquals(1, catalog.streams.size)
    val stream = catalog.streams[0]
    assertEquals("users", stream.stream.name)
    assertEquals("custom_users_table", stream.destinationObjectName)
  }

  @Test
  fun `test deserialization from JSON string without destinationObjectName`() {
    val jsonWithoutDestinationObjectName =
      """
      {
        "streams": [
          {
            "stream": {
              "name": "products",
              "json_schema": {},
              "supported_sync_modes": ["incremental"]
            },
            "sync_mode": "incremental",
            "destination_sync_mode": "append",
            "cursor_field": ["updated_at"]
          }
        ]
      }
      """.trimIndent()

    val catalog = Jsons.deserialize(jsonWithoutDestinationObjectName, ConfiguredAirbyteCatalog::class.java)

    assertEquals(1, catalog.streams.size)
    val stream = catalog.streams[0]
    assertEquals("products", stream.stream.name)
    assertEquals(null, stream.destinationObjectName)
    assertEquals(listOf("updated_at"), stream.cursorField)
  }

  @Test
  fun `test round trip serialization with complex nested data`() {
    val complexStream =
      ConfiguredAirbyteStream(
        stream = createAirbyteStreamWithComplexSchema(),
        syncMode = SyncMode.INCREMENTAL,
        destinationSyncMode = DestinationSyncMode.APPEND_DEDUP,
        cursorField = listOf("updated_at"),
        primaryKey = listOf(listOf("id"), listOf("tenant_id")),
        generationId = 12345L,
        minimumGenerationId = 10000L,
        syncId = 67890L,
        fields =
          listOf(
            Field("id", FieldType.INTEGER),
            Field("name", FieldType.STRING),
            Field("metadata", FieldType.OBJECT),
          ),
        mappers = listOf(createTestMapperConfig()),
        includeFiles = true,
        destinationObjectName = "complex_destination_table",
      )

    val catalog = ConfiguredAirbyteCatalog(listOf(complexStream))

    // First serialization
    val json1 = Jsons.serialize(catalog)
    val deserialized1 = Jsons.deserialize(json1, ConfiguredAirbyteCatalog::class.java)

    // Second serialization (round-trip)
    val json2 = Jsons.serialize(deserialized1)
    val deserialized2 = Jsons.deserialize(json2, ConfiguredAirbyteCatalog::class.java)

    // Both JSON strings should be identical
    assertEquals(json1, json2)

    // Both deserialized objects should be identical
    val stream1 = deserialized1.streams[0]
    val stream2 = deserialized2.streams[0]

    assertEquals(stream1.destinationObjectName, stream2.destinationObjectName)
    assertEquals(stream1.generationId, stream2.generationId)
    assertEquals(stream1.minimumGenerationId, stream2.minimumGenerationId)
    assertEquals(stream1.syncId, stream2.syncId)
    assertEquals(stream1.includeFiles, stream2.includeFiles)
    assertEquals(stream1.fields?.size, stream2.fields?.size)
    assertEquals(stream1.mappers.size, stream2.mappers.size)
  }

  private fun createFullConfiguredAirbyteCatalog(): ConfiguredAirbyteCatalog {
    val stream =
      ConfiguredAirbyteStream(
        stream = createAirbyteStreamWithSchema(),
        syncMode = SyncMode.INCREMENTAL,
        destinationSyncMode = DestinationSyncMode.APPEND_DEDUP,
        cursorField = listOf("updated_at"),
        primaryKey = listOf(listOf("id")),
        generationId = 123L,
        minimumGenerationId = 100L,
        syncId = 456L,
        fields = listOf(Field("test_field", FieldType.STRING)),
        mappers = listOf(createTestMapperConfig()),
        includeFiles = true,
        destinationObjectName = "test_destination_table",
      )

    return ConfiguredAirbyteCatalog(listOf(stream))
  }

  private fun createBasicAirbyteStream(): AirbyteStream =
    AirbyteStream(
      name = "test_stream",
      jsonSchema = Jsons.emptyObject(),
      supportedSyncModes = listOf(SyncMode.FULL_REFRESH),
      namespace = "test_namespace",
    )

  private fun createAirbyteStreamWithSchema(): AirbyteStream {
    val schema =
      Jsons.deserialize(
        """
        {
          "type": "object",
          "properties": {
            "id": {"type": "integer"},
            "name": {"type": "string"},
            "updated_at": {"type": "string", "format": "date-time"}
          }
        }
        """.trimIndent(),
      )

    return AirbyteStream(
      name = "test_stream_with_schema",
      jsonSchema = schema,
      supportedSyncModes = listOf(SyncMode.INCREMENTAL, SyncMode.FULL_REFRESH),
      namespace = "test_namespace",
      sourceDefinedCursor = true,
      defaultCursorField = listOf("updated_at"),
      sourceDefinedPrimaryKey = listOf(listOf("id")),
    )
  }

  private fun createAirbyteStreamWithComplexSchema(): AirbyteStream {
    val complexSchema =
      Jsons.deserialize(
        """
        {
          "type": "object",
          "properties": {
            "id": {"type": "integer"},
            "tenant_id": {"type": "integer"},
            "name": {"type": "string"},
            "metadata": {
              "type": "object",
              "properties": {
                "tags": {"type": "array", "items": {"type": "string"}},
                "settings": {"type": "object"}
              }
            },
            "updated_at": {"type": "string", "format": "date-time"}
          }
        }
        """.trimIndent(),
      )

    return AirbyteStream(
      name = "complex_stream",
      jsonSchema = complexSchema,
      supportedSyncModes = listOf(SyncMode.INCREMENTAL, SyncMode.FULL_REFRESH),
      namespace = "complex_namespace",
      sourceDefinedCursor = true,
      defaultCursorField = listOf("updated_at"),
      sourceDefinedPrimaryKey = listOf(listOf("id"), listOf("tenant_id")),
      isResumable = true,
      isFileBased = false,
    )
  }

  private fun createTestMapperConfig(): TestMapperConfig =
    TestMapperConfig(
      config =
        TestConfig(
          field1 = "test_value",
          enumField = TestEnums.ONE,
          field2 = "another_test_value",
        ),
      id = UUID.randomUUID(),
    )
}
