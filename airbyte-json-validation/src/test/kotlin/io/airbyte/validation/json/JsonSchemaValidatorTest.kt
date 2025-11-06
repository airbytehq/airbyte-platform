/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.validation.json

import io.airbyte.commons.io.IOs.writeFile
import io.airbyte.commons.json.Jsons.deserialize
import io.airbyte.validation.json.JsonSchemaValidator.Companion.getSchema
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.net.URI
import java.nio.file.Files

internal class JsonSchemaValidatorTest {
  @Test
  fun testValidateSuccess() {
    val validator = JsonSchemaValidator()

    val object1 = deserialize("{\"host\":\"abc\"}")
    assertTrue(validator.validate(VALID_SCHEMA, object1).isEmpty())
    assertDoesNotThrow({ validator.ensure(VALID_SCHEMA, object1) })

    val object2 = deserialize("{\"host\":\"abc\", \"port\":1}")
    assertTrue(validator.validate(VALID_SCHEMA, object2).isEmpty())
    assertDoesNotThrow({ validator.ensure(VALID_SCHEMA, object2) })
  }

  @Test
  fun testValidateFail() {
    val validator = JsonSchemaValidator()

    val object1 = deserialize("{}")
    assertFalse(validator.validate(VALID_SCHEMA, object1).isEmpty())
    assertThrows(JsonValidationException::class.java, { validator.ensure(VALID_SCHEMA, object1) })

    val object2 = deserialize("{\"host\":\"abc\", \"port\":9999999}")
    assertFalse(validator.validate(VALID_SCHEMA, object2).isEmpty())
    assertThrows(JsonValidationException::class.java, { validator.ensure(VALID_SCHEMA, object2) })
  }

  @Test
  fun test() {
    val schema = (
      "{\n" +
        "  \"\$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
        "  \"title\": \"OuterObject\",\n" +
        "  \"type\": \"object\",\n" +
        "  \"properties\": {\n" +
        "    \"field1\": {\n" +
        "      \"type\": \"string\"\n" +
        "    }\n" +
        "  },\n" +
        "  \"definitions\": {\n" +
        "    \"InnerObject\": {\n" +
        "      \"type\": \"object\",\n" +
        "      \"properties\": {\n" +
        "        \"field2\": {\n" +
        "          \"type\": \"string\"\n" +
        "        }\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}\n"
    )

    val schemaFile = Files.createTempDirectory("test").resolve("schema.json")
    writeFile(schemaFile, schema)

    // outer object
    assertTrue(getSchema(schemaFile.toFile()).get(PROPERTIES).has("field1"))
    assertFalse(getSchema(schemaFile.toFile()).get(PROPERTIES).has("field2"))
    // inner object
    assertTrue(getSchema(schemaFile.toFile(), "InnerObject").get(PROPERTIES).has("field2"))
    assertFalse(getSchema(schemaFile.toFile(), "InnerObject").get(PROPERTIES).has("field1"))
    // non-existent object
    assertThrows(
      IllegalArgumentException::class.java,
      { getSchema(schemaFile.toFile(), "NonExistentObject") },
    )
  }

  @Test
  fun testResolveReferences() {
    val referencableSchemas =
      """
      {
        "definitions": {
          "ref1": {"type": "string"},
          "ref2": {"type": "boolean"}
        }
      }
      
      """.trimIndent()
    val schemaFile = Files.createTempDirectory("test").resolve("WellKnownTypes.json")
    writeFile(schemaFile, referencableSchemas)
    val jsonSchemaValidator =
      JsonSchemaValidator(URI("file://" + schemaFile.toFile().getParentFile().getAbsolutePath() + "/foo.json"))

    val validationResult: Set<String> =
      jsonSchemaValidator.validate(
        deserialize(
          """
          {
            "type": "object",
            "properties": {
              "prop1": {"${'$'}ref": "WellKnownTypes.json#/definitions/ref1"},
              "prop2": {"${'$'}ref": "WellKnownTypes.json#/definitions/ref2"}
            }
          }
          
          """.trimIndent(),
        ),
        deserialize(
          """
          {
            "prop1": "foo",
            "prop2": "false"
          }
          
          """.trimIndent(),
        ),
      )

    assertEquals(setOf("$.prop2: string found, boolean expected"), validationResult)
  }

  @Test
  fun testIntializedMethodsShouldErrorIfNotInitialised() {
    val validator = JsonSchemaValidator()

    assertThrows(
      IllegalArgumentException::class.java,
      { validator.testInitializedSchema("uninitialised", deserialize("{}")) },
    )
    assertThrows(
      IllegalArgumentException::class.java,
      { validator.validateInitializedSchema("uninitialised", deserialize("{}")) },
    )
  }

  @Test
  fun testIntializedMethodsShouldValidateIfInitialised() {
    val validator = JsonSchemaValidator()
    val schemaName = "schema_name"
    val goodJson = deserialize("{\"host\":\"abc\"}")

    validator.initializeSchemaValidator(schemaName, VALID_SCHEMA)

    assertTrue(validator.testInitializedSchema(schemaName, goodJson))
    assertDoesNotThrow({ validator.validateInitializedSchema(schemaName, goodJson) })

    val badJson = deserialize("{\"host\":1}")
    assertFalse(validator.testInitializedSchema(schemaName, badJson))

    val errorMessages: Set<String> = validator.validateInitializedSchema(schemaName, badJson)
    assert(!errorMessages.isEmpty())
  }

  companion object {
    private const val PROPERTIES = "properties"

    private val VALID_SCHEMA =
      deserialize(
        (
          "{\n" +
            "    \"\$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
            "    \"title\": \"test\",\n" +
            "    \"type\": \"object\",\n" +
            "    \"required\": [\"host\"],\n" +
            "    \"additionalProperties\": false,\n" +
            "    \"properties\": {\n" +
            "      \"host\": {\n" +
            "        \"type\": \"string\"\n" +
            "      },\n" +
            "      \"port\": {\n" +
            "        \"type\": \"integer\",\n" +
            "        \"minimum\": 0,\n" +
            "        \"maximum\": 65536\n" +
            "      }" +
            "    }\n" +
            "  }"
        ),
      )
  }
}
