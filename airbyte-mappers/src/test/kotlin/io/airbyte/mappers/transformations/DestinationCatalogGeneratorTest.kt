/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.mappers.transformations

import TEST_OBJECT_MAPPER
import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.config.AirbyteStream
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.Field
import io.airbyte.config.FieldType
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.mapper.configs.TEST_MAPPER_NAME
import io.airbyte.config.mapper.configs.TestConfig
import io.airbyte.config.mapper.configs.TestEnums
import io.airbyte.config.mapper.configs.TestMapperConfig
import io.airbyte.mappers.mocks.FailingTestMapper
import io.airbyte.mappers.mocks.TestMapper
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class DestinationCatalogGeneratorTest {
  private val destinationCatalogGeneratorWithoutMapper = DestinationCatalogGenerator(listOf(), TEST_OBJECT_MAPPER)
  private val destinationCatalogGeneratorWithMapper = DestinationCatalogGenerator(listOf(TestMapper()), TEST_OBJECT_MAPPER)
  private val destinationCatalogGeneratorWithFailingMapper = DestinationCatalogGenerator(listOf(FailingTestMapper()), TEST_OBJECT_MAPPER)

  @Test
  fun `test generateDestinationCatalogWithoutMapper`() {
    val configuredUsersStream =
      ConfiguredAirbyteStream(
        stream =
          AirbyteStream(
            name = "users",
            jsonSchema =
              TEST_OBJECT_MAPPER.valueToTree(
                mapOf(
                  "type" to "object",
                  "${'$'}schema" to "http://json-schema.org/schema#",
                  "properties" to mapOf("field1" to mapOf("type" to "string")),
                  "additionalProperties" to true,
                ),
              ),
            supportedSyncModes = listOf(),
          ),
        fields =
          listOf(
            Field(name = "field1", type = FieldType.STRING),
          ),
      )

    val catalog = ConfiguredAirbyteCatalog(streams = listOf(configuredUsersStream))

    val catalogCopy = destinationCatalogGeneratorWithoutMapper.generateDestinationCatalog(catalog)

    assertEquals(
      catalog.streams[0].stream.jsonSchema,
      catalogCopy.catalog.streams[0]
        .stream.jsonSchema,
    )
  }

  @Test
  fun testPreserveInitialCatalog() {
    val catalogJson =
      """
            {
        "streams": [
          {
            "fields": [
              {
                "name": "id",
                "type": "STRING"
              },
              {
                "name": "name",
                "type": "STRING"
              },
              {
                "name": "type",
                "type": "STRING"
              },
              {
                "name": "remote_id",
                "type": "UNKNOWN"
              },
              {
                "name": "created_at",
                "type": "STRING"
              },
              {
                "name": "modified_at",
                "type": "STRING"
              },
              {
                "name": "remote_data",
                "type": "UNKNOWN"
              },
              {
                "name": "parent_group",
                "type": "UNKNOWN"
              },
              {
                "name": "field_mappings",
                "type": "UNKNOWN"
              },
              {
                "name": "remote_was_deleted",
                "type": "BOOLEAN"
              }
            ],
            "stream": {
              "name": "groups",
              "json_schema": {
                "type": "object",
                "${'$'}schema": "http://json-schema.org/schema#",
                "properties": {
                  "id": {
                    "type": "string"
                  },
                  "name": {
                    "type": "string"
                  },
                  "type": {
                    "type": "string"
                  },
                  "remote_id": {
                    "type": "null"
                  },
                  "created_at": {
                    "type": "string"
                  },
                  "modified_at": {
                    "type": "string"
                  },
                  "remote_data": {
                    "type": "null"
                  },
                  "parent_group": {
                    "type": "null"
                  },
                  "field_mappings": {
                    "type": "null"
                  },
                  "remote_was_deleted": {
                    "type": "boolean"
                  }
                },
                "additionalProperties": true
              },
              "default_cursor_field": [
                "modified_at"
              ],
              "supported_sync_modes": [
                "full_refresh",
                "incremental"
              ],
              "source_defined_cursor": true,
              "source_defined_primary_key": [
                [
                  "id"
                ]
              ]
            },
            "mappers": [],
            "sync_mode": "incremental",
            "primary_key": [
              [
                "id"
              ]
            ],
            "cursor_field": [
              "modified_at"
            ],
            "destination_sync_mode": "append"
          }
        ]
      }
      """.trimIndent()
    val catalogParsed = TEST_OBJECT_MAPPER.readValue(catalogJson, ConfiguredAirbyteCatalog::class.java)
    val catalogGenerated = destinationCatalogGeneratorWithoutMapper.generateDestinationCatalog(catalogParsed)

    assertEquals(catalogParsed, catalogGenerated.catalog)
  }

  @Test
  fun testMappersCanUpdateCursorAndPK() {
    val catalogJson =
      """
            {
        "streams": [
          {
            "fields": [
              {
                "name": "id",
                "type": "STRING"
              },
              {
                "name": "name",
                "type": "STRING"
              },
              {
                "name": "modified_at",
                "type": "STRING"
              }
            ],
            "stream": {
              "name": "groups",
              "json_schema": {
                "type": "object",
                "${'$'}schema": "http://json-schema.org/schema#",
                "properties": {
                  "id": {
                    "type": "string"
                  },
                  "name": {
                    "type": "string"
                  },
                  "modified_at": {
                    "type": "string"
                  }
                },
                "additionalProperties": true
              },
              "default_cursor_field": [
                "modified_at"
              ],
              "supported_sync_modes": [
                "full_refresh",
                "incremental"
              ],
              "source_defined_cursor": true,
              "source_defined_primary_key": [
                [
                  "id"
                ]
              ]
            },
            "mappers": [{"name": "test-mapper", "config": {"field1": "random_value", "field2": "random_value", "enumField": "ONE"}}],
            "sync_mode": "incremental",
            "primary_key": [
              [
                "id"
              ]
            ],
            "cursor_field": [
              "modified_at"
            ],
            "destination_sync_mode": "append"
          }
        ]
      }
      """.trimIndent()

    val expectedCatalogJson =
      """
            {
        "streams": [
          {
            "fields": [
              {
                "name": "id_test",
                "type": "STRING"
              },
              {
                "name": "name_test",
                "type": "STRING"
              },
              {
                "name": "modified_at_test",
                "type": "STRING"
              }
            ],
            "stream": {
              "name": "groups",
              "json_schema": {
                "type": "object",
                "${'$'}schema": "http://json-schema.org/schema#",
                "properties": {
                  "id_test": {
                    "type": "string"
                  },
                  "name_test": {
                    "type": "string"
                  },
                  "modified_at_test": {
                    "type": "string"
                  }
                },
                "additionalProperties": true
              },
              "default_cursor_field": [
                "modified_at_test"
              ],
              "supported_sync_modes": [
                "full_refresh",
                "incremental"
              ],
              "source_defined_cursor": true,
              "source_defined_primary_key": [
                [
                  "id_test"
                ]
              ]
            },
            "mappers": [{"name": "test-mapper", "config": {"field1": "random_value", "field2": "random_value", "enumField": "ONE"}}],
            "sync_mode": "incremental",
            "primary_key": [
              [
                "id_test"
              ]
            ],
            "cursor_field": [
              "modified_at_test"
            ],
            "destination_sync_mode": "append"
          }
        ]
      }
      """.trimIndent()
    val catalogParsed = TEST_OBJECT_MAPPER.readValue(catalogJson, ConfiguredAirbyteCatalog::class.java)
    val catalogGenerated = destinationCatalogGeneratorWithMapper.generateDestinationCatalog(catalogParsed)

    assertEquals(TEST_OBJECT_MAPPER.readValue(expectedCatalogJson, ConfiguredAirbyteCatalog::class.java), catalogGenerated.catalog)
  }

  @Test
  fun `test generateDestinationCatalogMissingMapper`() {
    val mapperConfig = TestMapperConfig(TEST_MAPPER_NAME, null, null, TestConfig("", TestEnums.ONE, ""))
    val configuredUsersStream =
      ConfiguredAirbyteStream(
        stream =
          AirbyteStream(
            name = "users",
            jsonSchema =
              TEST_OBJECT_MAPPER.valueToTree(
                mapOf(
                  "type" to "object",
                  "${'$'}schema" to "http://json-schema.org/schema#",
                  "properties" to mapOf("field1" to mapOf("type" to "string")),
                ),
              ),
            supportedSyncModes = listOf(),
          ),
        fields =
          listOf(
            Field(name = "field1", type = FieldType.STRING),
          ),
        mappers = listOf(mapperConfig),
      )

    val catalog = ConfiguredAirbyteCatalog(streams = listOf(configuredUsersStream))

    val result = destinationCatalogGeneratorWithoutMapper.generateDestinationCatalog(catalog)

    assertTrue(
      result.catalog.streams[0]
        .mappers
        .isEmpty(),
    )
    val streamDescriptor = StreamDescriptor().withName("users")
    assertEquals(DestinationCatalogGenerator.MapperErrorType.MISSING_MAPPER, result.errors[streamDescriptor]?.get(mapperConfig)?.type)
  }

  @Test
  fun `test generateDestinationCatalogFailedSchema`() {
    val mapperConfig = TestMapperConfig(TEST_MAPPER_NAME, null, null, TestConfig("field1", TestEnums.ONE, "field2"))
    val configuredUsersStream =
      ConfiguredAirbyteStream(
        stream =
          AirbyteStream(
            name = "users",
            jsonSchema =
              TEST_OBJECT_MAPPER.valueToTree(
                mapOf(
                  "type" to "object",
                  "${'$'}schema" to "http://json-schema.org/schema#",
                  "properties" to mapOf("field1" to mapOf("type" to "string")),
                ),
              ),
            supportedSyncModes = listOf(),
          ),
        fields =
          listOf(
            Field(name = "field1", type = FieldType.STRING),
          ),
        mappers = listOf(mapperConfig),
      )

    val catalog = ConfiguredAirbyteCatalog(streams = listOf(configuredUsersStream))

    val result = destinationCatalogGeneratorWithFailingMapper.generateDestinationCatalog(catalog)

    assertTrue(
      result.catalog.streams[0]
        .mappers
        .isEmpty(),
    )
    val streamDescriptor = StreamDescriptor().withName("users")
    assertEquals(DestinationCatalogGenerator.MapperErrorType.INVALID_MAPPER_CONFIG, result.errors[streamDescriptor]?.get(mapperConfig)?.type)
  }

  @Test
  fun `test generateDestinationFieldsWithoutMapper`() {
    val configuredUsersStream =
      ConfiguredAirbyteStream(
        stream =
          AirbyteStream(
            name = "users",
            jsonSchema = TEST_OBJECT_MAPPER.valueToTree(emptyMap<String, Any>()),
            supportedSyncModes = listOf(),
          ),
        fields =
          listOf(
            Field(name = "field1_1", type = FieldType.STRING),
            Field(name = "field1_2", type = FieldType.DATE),
          ),
      )

    val resultFields = destinationCatalogGeneratorWithoutMapper.applyMapperToFields(configuredUsersStream).slimStream.fields

    assertEquals(configuredUsersStream.fields, resultFields)
  }

  @Test
  fun `test generateDestinationFieldsWithMappers`() {
    val configuredUsersStream =
      ConfiguredAirbyteStream(
        stream =
          AirbyteStream(
            name = "users",
            jsonSchema = TEST_OBJECT_MAPPER.valueToTree(emptyMap<String, Any>()),
            supportedSyncModes = listOf(),
          ),
        fields =
          listOf(
            Field(name = "field1_1", type = FieldType.STRING),
            Field(name = "field1_2", type = FieldType.DATE),
          ),
        mappers =
          listOf(
            TestMapperConfig(TEST_MAPPER_NAME, null, null, TestConfig("", TestEnums.ONE, "")),
            TestMapperConfig(TEST_MAPPER_NAME, null, null, TestConfig("", TestEnums.TWO, "")),
          ),
      )

    val resultFields = destinationCatalogGeneratorWithMapper.applyMapperToFields(configuredUsersStream).slimStream.fields

    assertEquals(
      listOf(
        Field(name = "field1_1_test_test", type = FieldType.STRING),
        Field(name = "field1_2_test_test", type = FieldType.STRING),
      ),
      resultFields,
    )
  }

  @Test
  fun `test generateDestinationCatalogWithMapper`() {
    val configuredUsersStream1 =
      ConfiguredAirbyteStream(
        stream =
          AirbyteStream(
            name = "users",
            jsonSchema =
              TEST_OBJECT_MAPPER.valueToTree(
                mapOf(
                  "type" to "object",
                  "${'$'}schema" to "http://json-schema.org/schema#",
                  "properties" to
                    mapOf(
                      "field1_1" to mapOf("type" to "string"),
                      "field1_2" to mapOf("type" to "string", "format" to "date"),
                    ),
                ),
              ),
            supportedSyncModes = listOf(),
          ),
        fields =
          listOf(
            Field(name = "field1_1", type = FieldType.STRING),
            Field(name = "field1_2", type = FieldType.DATE),
          ),
        mappers = listOf(TestMapperConfig(TEST_MAPPER_NAME, null, null, TestConfig("", TestEnums.TWO, ""))),
      )

    val configuredUsersStream2 =
      ConfiguredAirbyteStream(
        stream =
          AirbyteStream(
            name = "users",
            jsonSchema =
              TEST_OBJECT_MAPPER.valueToTree(
                mapOf(
                  "type" to "object",
                  "${'$'}schema" to "http://json-schema.org/schema#",
                  "properties" to
                    mapOf(
                      "field2_1" to mapOf("type" to "integer"),
                    ),
                ),
              ),
            supportedSyncModes = listOf(),
          ),
        fields =
          listOf(
            Field(name = "field2_1", type = FieldType.INTEGER),
          ),
        mappers = listOf(TestMapperConfig(TEST_MAPPER_NAME, null, null, TestConfig("", TestEnums.TWO, ""))),
      )

    val catalog = ConfiguredAirbyteCatalog(streams = listOf(configuredUsersStream1, configuredUsersStream2))

    val catalogCopy = destinationCatalogGeneratorWithMapper.generateDestinationCatalog(catalog)

    assertEquals(
      TEST_OBJECT_MAPPER.valueToTree(
        mapOf(
          "type" to "object",
          "${'$'}schema" to "http://json-schema.org/schema#",
          "properties" to
            mapOf(
              "field1_1_test" to mapOf("type" to "string"),
              "field1_2_test" to mapOf("type" to "string"),
            ),
          "additionalProperties" to true,
        ),
      ),
      catalogCopy.catalog.streams[0]
        .stream.jsonSchema,
    )
    assertEquals(
      TEST_OBJECT_MAPPER.valueToTree(
        mapOf(
          "type" to "object",
          "${'$'}schema" to "http://json-schema.org/schema#",
          "properties" to
            mapOf(
              "field2_1_test" to mapOf("type" to "string"),
            ),
          "additionalProperties" to true,
        ),
      ),
      catalogCopy.catalog.streams[1]
        .stream.jsonSchema,
    )
  }

  @Test
  fun `test fieldSerialization`() {
    val input =
      listOf(
        Field("fieldString", FieldType.STRING),
        Field("fieldBoolean", FieldType.BOOLEAN),
        Field("fieldInteger", FieldType.INTEGER),
        Field("fieldNumber", FieldType.NUMBER),
        Field("fieldDate", FieldType.DATE),
        Field("fieldTimestampWithoutTimezone", FieldType.TIMESTAMP_WITHOUT_TIMEZONE),
        Field("fieldTimestampWithTimezone", FieldType.TIMESTAMP_WITH_TIMEZONE),
        Field("fieldTimeWithoutTimezone", FieldType.TIME_WITHOUT_TIMEZONE),
        Field("fieldTimeWithTimezone", FieldType.TIME_WITH_TIMEZONE),
        Field("fieldObject", FieldType.OBJECT),
        Field("fieldArray", FieldType.OBJECT),
        Field("fieldMulti", FieldType.OBJECT),
        Field("fieldUnknown", FieldType.UNKNOWN),
      )
    val expectedOutputJson =
      TEST_OBJECT_MAPPER.readValue(
        """
      {
        "fieldString": {
          "type": "string"
        },
        "fieldBoolean": {
          "type": "boolean"
        },
        "fieldInteger": {
          "type": "integer"
        },
        "fieldNumber": {
          "type": "number"
        },
        "fieldDate": {
          "type": "string",
          "format": "date"
        },
        "fieldTimestampWithoutTimezone": {
          "type": "string",
          "format": "date-time",
          "airbyte_type": "timestamp_without_timezone"
        },
        "fieldTimestampWithTimezone": {
          "type": "string",
          "format": "date-time",
          "airbyte_type": "timestamp_with_timezone"
        },
        "fieldTimeWithoutTimezone": {
          "type": "string",
          "format": "time",
          "airbyte_type": "time_without_timezone"
        },
        "fieldTimeWithTimezone": {
          "type": "string",
          "format": "time",
          "airbyte_type": "time_with_timezone"
        },
        "fieldObject": {
          "type": "object"
        },
        "fieldArray": {
          "type": "array"
        },
        "fieldMulti": {
          "type": "oneOf"
        },
        "fieldUnknown": {
          "I": "don't",
          "follow": "specs"
        }
      }
    """,
        JsonNode::class.java,
      )

    val result =
      destinationCatalogGeneratorWithoutMapper.generateJsonSchemaFromFields(
        input,
        TEST_OBJECT_MAPPER.valueToTree(
          mapOf(
            "properties" to
              mapOf(
                "fieldObject" to mapOf("type" to "object"),
                "fieldArray" to mapOf("type" to "array"),
                "fieldMulti" to mapOf("type" to "oneOf"),
                "fieldUnknown" to mapOf("I" to "don't", "follow" to "specs"),
              ),
          ),
        ),
      )
    assertEquals(expectedOutputJson, TEST_OBJECT_MAPPER.readValue(result, JsonNode::class.java))
  }
}
