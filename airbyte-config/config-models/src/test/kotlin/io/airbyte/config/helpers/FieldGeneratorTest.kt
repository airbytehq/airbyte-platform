/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.config.Field
import io.airbyte.config.FieldType
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource

class FieldGeneratorTest {
  private val fieldGenerator = FieldGenerator()

  @ParameterizedTest
  @ValueSource(
    strings = [
      """
      {
        "type": "object",
        "required": [
          "key1",
          "key2",
          "key3",
          "key4",
          "key5"
        ],
        "properties": {
          "key1": {
            "type": "boolean"
          },
          "key2": {
            "type": "integer"
          },
          "key3": {
            "type": "number",
            "airbyte_type": "integer"
          },
          "key4": {
            "type": "number"
          },
          "key5": {
            "type": "string",
            "format": "date"
          },
          "key6": {
            "type": "string",
            "format": "time",
            "airbyte_type": "time_without_timezone"
          },
          "key7": {
            "type": "string",
            "format": "time",
            "airbyte_type": "time_with_timezone"
          },
          "key8": {
            "type": "string",
            "format": "date-time",
            "airbyte_type": "timestamp_without_timezone"
          },
          "key9": {
            "type": "string",
            "format": "date-time",
            "airbyte_type": "timestamp_with_timezone"
          },
          "key10": {
            "type": "string",
            "format": "date-time"
          },
          "key11": {
            "type": "string"
          },
          "key12": {
            "type": "object"
          },
          "key13": {
            "oneOf": [
              {
                "type": "string"
              },
              {
                "type": "integer"
              }
            ]
          },
          "key14": {
            "type": "array"
          },
          "key15": {
            "anyOf": [
              {
                "type": "string"
              },
              {
                "type": "integer"
              }
            ]
          },
          "key16": {
            "no": "type"
          }
        }
      }
    """,
      """
      {
        "type": ["object"],
        "required": [
          "key1",
          "key2",
          "key3",
          "key4",
          "key5"
        ],
        "properties": {
          "key1": {
            "type": ["boolean"]
          },
          "key2": {
            "type": ["integer"]
          },
          "key3": {
            "type": ["number"],
            "airbyte_type": "integer"
          },
          "key4": {
            "type": ["number"]
          },
          "key5": {
            "type": ["string"],
            "format": "date"
          },
          "key6": {
            "type": ["string"],
            "format": "time",
            "airbyte_type": "time_without_timezone"
          },
          "key7": {
            "type": ["string"],
            "format": "time",
            "airbyte_type": "time_with_timezone"
          },
          "key8": {
            "type": ["string"],
            "format": "date-time",
            "airbyte_type": "timestamp_without_timezone"
          },
          "key9": {
            "type": ["string"],
            "format": "date-time",
            "airbyte_type": "timestamp_with_timezone"
          },
          "key10": {
            "type": ["string"],
            "format": "date-time"
          },
          "key11": {
            "type": ["string"]
          },
          "key12": {
            "type": ["object"]
          },
          "key13": {
            "type": ["string", "integer"]
          },
          "key14": {
            "type": ["array"]
          }
        }
      }
    """,
      """
      {
        "type": ["null", "object"],
        "required": [
          "key1",
          "key2",
          "key3",
          "key4",
          "key5"
        ],
        "properties": {
          "key1": {
            "type": ["null", "boolean"]
          },
          "key2": {
            "type": ["null", "integer"]
          },
          "key3": {
            "type": ["null", "number"],
            "airbyte_type": "integer"
          },
          "key4": {
            "type": ["null", "number"]
          },
          "key5": {
            "type": ["null", "string"],
            "format": "date"
          },
          "key6": {
            "type": ["null", "string"],
            "format": "time",
            "airbyte_type": "time_without_timezone"
          },
          "key7": {
            "type": ["null", "string"],
            "format": "time",
            "airbyte_type": "time_with_timezone"
          },
          "key8": {
            "type": ["null", "string"],
            "format": "date-time",
            "airbyte_type": "timestamp_without_timezone"
          },
          "key9": {
            "type": ["null", "string"],
            "format": "date-time",
            "airbyte_type": "timestamp_with_timezone"
          },
          "key10": {
            "type": ["null", "string"],
            "format": "date-time"
          },
          "key11": {
            "type": ["null", "string"]
          },
          "key12": {
            "type": ["null", "object"]
          },
          "key13": {
            "type": ["null", "string", "integer"]
          },
          "key14": {
            "type": ["null", "array"]
          }
        }
      }
    """,
    ],
  )
  fun `test field generation`(catalogSchema: String) {
    val jsonSchema = Jsons.deserialize(catalogSchema.trimIndent())

    val fields = fieldGenerator.getFieldsFromSchema(jsonSchema)

    fields.forEach {
      when (it.name) {
        "key1" -> assertEquals(Field("key1", FieldType.BOOLEAN, true), it)
        "key2" -> assertEquals(Field("key2", FieldType.INTEGER, true), it)
        "key3" -> assertEquals(Field("key3", FieldType.INTEGER, true), it)
        "key4" -> assertEquals(Field("key4", FieldType.NUMBER, true), it)
        "key5" -> assertEquals(Field("key5", FieldType.DATE, true), it)
        "key6" -> assertEquals(Field("key6", FieldType.TIME_WITHOUT_TIMEZONE), it)
        "key7" -> assertEquals(Field("key7", FieldType.TIME_WITH_TIMEZONE), it)
        "key8" -> assertEquals(Field("key8", FieldType.TIMESTAMP_WITHOUT_TIMEZONE), it)
        "key9" -> assertEquals(Field("key9", FieldType.TIMESTAMP_WITH_TIMEZONE), it)
        "key10" -> assertEquals(Field("key10", FieldType.TIMESTAMP_WITH_TIMEZONE), it)
        "key11" -> assertEquals(Field("key11", FieldType.STRING), it)
        "key12" -> assertEquals(Field("key12", FieldType.OBJECT), it)
        "key13" -> assertEquals(Field("key13", FieldType.MULTI), it)
        "key14" -> assertEquals(Field("key14", FieldType.ARRAY), it)
        "key15" -> assertEquals(Field("key15", FieldType.MULTI), it)
        "key16" -> assertEquals(Field("key16", FieldType.UNKNOWN), it)
        else -> throw IllegalStateException("Unexpected field: ${it.name}")
      }
    }
  }

  @ParameterizedTest
  @MethodSource("getNodeAndExpectedType")
  fun `test node to field`(
    node: JsonNode,
    expectedType: FieldType,
  ) {
    val result = fieldGenerator.getFieldTypeFromNode(node)
    Assertions.assertEquals(expectedType, result)
  }

  @Test
  fun `test invalid type`() {
    val result = fieldGenerator.getFieldTypeFromSchemaType("not_supported", Jsons.emptyObject())

    assertEquals(FieldType.UNKNOWN, result)
  }

  @Test
  fun `test invalid string type`() {
    val result =
      fieldGenerator.getFieldTypeFromSchemaType(
        "string",
        Jsons.deserialize(
          """
          {"type": "string", "format": "invalid"}
          """.trimIndent(),
        ),
      )

    assertEquals(FieldType.UNKNOWN, result)
  }

  @Test
  fun `test null filtering`() {
    val arrayNode = Jsons.arrayNode()
    arrayNode.add("null")
    arrayNode.add("string")

    val result = fieldGenerator.removeNullFromArray(arrayNode)

    assertEquals(listOf("string"), result)
  }

  @Test
  fun `test null filtering doesn't filter non null`() {
    val arrayNode = Jsons.arrayNode()
    arrayNode.add("integer")
    arrayNode.add("string")

    val result = fieldGenerator.removeNullFromArray(arrayNode)

    assertEquals(listOf("integer", "string"), result)
  }

  companion object {
    @JvmStatic
    private fun getNodeAndExpectedType(): List<Arguments> =
      listOf(
        Arguments.of(
          Jsons.deserialize(
            """{
            "type": "boolean"
          }""",
          ),
          FieldType.BOOLEAN,
        ),
        Arguments.of(
          Jsons.deserialize(
            """{
            "type": "integer"
          }""",
          ),
          FieldType.INTEGER,
        ),
        Arguments.of(
          Jsons.deserialize(
            """{
            "type": "number"
          }""",
          ),
          FieldType.NUMBER,
        ),
        Arguments.of(
          Jsons.deserialize(
            """{
            "type": "number", "airbyte_type": "integer"
          }""",
          ),
          FieldType.INTEGER,
        ),
        Arguments.of(
          Jsons.deserialize(
            """{
            "type": "string", "format": "date"
          }""",
          ),
          FieldType.DATE,
        ),
        Arguments.of(
          Jsons.deserialize(
            """{
            "type": "string", "format": "time", "airbyte_type": "time_without_timezone"
          }""",
          ),
          FieldType.TIME_WITHOUT_TIMEZONE,
        ),
        Arguments.of(
          Jsons.deserialize(
            """{
            "type": "string", "format": "time", "airbyte_type": "time_with_timezone"
          }""",
          ),
          FieldType.TIME_WITH_TIMEZONE,
        ),
        Arguments.of(
          Jsons.deserialize(
            """{
            "type": "string", "format": "date-time", "airbyte_type": "timestamp_without_timezone"
          }""",
          ),
          FieldType.TIMESTAMP_WITHOUT_TIMEZONE,
        ),
        Arguments.of(
          Jsons.deserialize(
            """{
            "type": "string", "format": "date-time", "airbyte_type": "timestamp_with_timezone"
          }""",
          ),
          FieldType.TIMESTAMP_WITH_TIMEZONE,
        ),
        Arguments.of(
          Jsons.deserialize(
            """{
            "type": "string", "format": "date-time"
          }""",
          ),
          FieldType.TIMESTAMP_WITH_TIMEZONE,
        ),
        Arguments.of(
          Jsons.deserialize(
            """{
            "type": "string"
          }""",
          ),
          FieldType.STRING,
        ),
        Arguments.of(
          Jsons.deserialize(
            """{
            "type": "object"
          }""",
          ),
          FieldType.OBJECT,
        ),
        Arguments.of(
          Jsons.deserialize(
            """{
            "type": "array"
          }""",
          ),
          FieldType.ARRAY,
        ),
      )
  }
}
