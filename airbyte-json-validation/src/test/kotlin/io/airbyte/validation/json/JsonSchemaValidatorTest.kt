/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.validation.json

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
import kotlin.io.path.writeText

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
  fun `partial validation allows missing required fields but validates supplied fields`() {
    val validator = JsonSchemaValidator()

    assertDoesNotThrow { validator.ensurePartial(PARTIAL_SCHEMA, deserialize("{}")) }
    assertDoesNotThrow { validator.ensurePartial(PARTIAL_SCHEMA, deserialize("""{"host":"db.example.com"}""")) }

    assertThrows(JsonValidationException::class.java) {
      validator.ensurePartial(PARTIAL_SCHEMA, deserialize("""{"host":42}"""))
    }
    assertThrows(JsonValidationException::class.java) {
      validator.ensurePartial(PARTIAL_SCHEMA, deserialize("""{"mode":"unsupported"}"""))
    }
    assertThrows(JsonValidationException::class.java) {
      validator.ensurePartial(PARTIAL_SCHEMA, deserialize("""{"port":0}"""))
    }
    assertThrows(JsonValidationException::class.java) {
      validator.ensurePartial(PARTIAL_SCHEMA, deserialize("""{"tags":[]}"""))
    }
    assertThrows(JsonValidationException::class.java) {
      validator.ensurePartial(PARTIAL_SCHEMA, deserialize("""{"options":{"unknown":true}}"""))
    }
    assertThrows(JsonValidationException::class.java) {
      validator.ensurePartial(PARTIAL_SCHEMA, deserialize("""{"unknown":true}"""))
    }
  }

  @Test
  fun `partial validation permits an unselected oneOf and requires a valid selector for branch data`() {
    val validator = JsonSchemaValidator()

    assertDoesNotThrow { validator.ensurePartial(ONE_OF_SCHEMA, deserialize("""{"credentials":{}}""")) }
    assertDoesNotThrow {
      validator.ensurePartial(
        ONE_OF_SCHEMA,
        deserialize("""{"credentials":{"auth_type":"api_key","api_key":"secret"}}"""),
      )
    }
    assertDoesNotThrow {
      validator.ensurePartial(ONE_OF_SCHEMA, deserialize("""{"credentials":{"api_key":"secret"}}"""))
    }
    assertThrows(JsonValidationException::class.java) {
      validator.ensurePartial(ONE_OF_SCHEMA, deserialize("""{"credentials":{"auth_type":"invalid"}}"""))
    }
    assertThrows(JsonValidationException::class.java) {
      validator.ensurePartial(
        ONE_OF_SCHEMA,
        deserialize(
          """{"credentials":{"auth_type":"api_key","api_key":"secret","client_id":"client","client_secret":"secret"}}""",
        ),
      )
    }
    assertThrows(JsonValidationException::class.java) {
      validator.ensurePartial(ONE_OF_SCHEMA, deserialize("""{"credentials":{"unknown":"value"}}"""))
    }
  }

  @Test
  fun `partial validation uses the original oneOf rules`() {
    val validator = JsonSchemaValidator()

    listOf(OPTIONAL_SELECTOR_ONE_OF_SCHEMA, REFERENCED_OPTIONAL_SELECTOR_ONE_OF_SCHEMA).forEach { schema ->
      assertDoesNotThrow { validator.ensurePartial(schema, deserialize("""{"auth_method":{}}""")) }
      assertDoesNotThrow {
        validator.ensurePartial(schema, deserialize("""{"auth_method":{"mode":"api_key_auth","api_key":"secret"}}"""))
      }
      assertDoesNotThrow { validator.ensurePartial(schema, deserialize("""{"auth_method":{"mode":"no_auth"}}""")) }
      assertThrows(JsonValidationException::class.java) {
        validator.ensurePartial(schema, deserialize("""{"auth_method":{"mode":"api_key_auth","api_key":42}}"""))
      }
    }

    val configWithoutSelector = deserialize("""{"auth_method":{"api_key":"secret"}}""")
    assertThrows(JsonValidationException::class.java) {
      validator.ensurePartial(OPTIONAL_SELECTOR_ONE_OF_SCHEMA, configWithoutSelector)
    }
    assertDoesNotThrow { validator.ensurePartial(REFERENCED_OPTIONAL_SELECTOR_ONE_OF_SCHEMA, configWithoutSelector) }

    val mixedBranch = deserialize("""{"auth_method":{"mode":"no_auth","api_key":"secret"}}""")
    val unknownField = deserialize("""{"auth_method":{"unknown":"value"}}""")
    assertDoesNotThrow { validator.ensurePartial(OPTIONAL_SELECTOR_ONE_OF_SCHEMA, mixedBranch) }
    assertDoesNotThrow { validator.ensurePartial(OPTIONAL_SELECTOR_ONE_OF_SCHEMA, unknownField) }
    assertThrows(JsonValidationException::class.java) {
      validator.ensurePartial(REFERENCED_OPTIONAL_SELECTOR_ONE_OF_SCHEMA, mixedBranch)
    }
    assertThrows(JsonValidationException::class.java) {
      validator.ensurePartial(REFERENCED_OPTIONAL_SELECTOR_ONE_OF_SCHEMA, unknownField)
    }
  }

  @Test
  fun `partial validation allows a selected referenced oneOf branch to omit required data`() {
    val validator = JsonSchemaValidator()
    val selectedBranch = deserialize("""{"auth_method":{"mode":"api_key_auth"}}""")

    assertThrows(JsonValidationException::class.java) {
      validator.ensure(REFERENCED_OPTIONAL_SELECTOR_ONE_OF_SCHEMA, selectedBranch)
    }

    assertDoesNotThrow {
      validator.ensurePartial(REFERENCED_OPTIONAL_SELECTOR_ONE_OF_SCHEMA, selectedBranch)
    }
    assertThrows(JsonValidationException::class.java) {
      validator.ensure(REFERENCED_OPTIONAL_SELECTOR_ONE_OF_SCHEMA, selectedBranch)
    }
  }

  @Test
  fun `partial validation follows local reference chains without losing oneOf rules`() {
    val validator = JsonSchemaValidator()

    assertDoesNotThrow {
      validator.ensurePartial(
        CHAINED_REFERENCED_ONE_OF_SCHEMA,
        deserialize("""{"auth_method":{"mode":"api_key_auth"}}"""),
      )
    }
    assertDoesNotThrow {
      validator.ensurePartial(CHAINED_REFERENCED_ONE_OF_SCHEMA, deserialize("""{"auth_method":{"api_key":"secret"}}"""))
    }
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
    schemaFile.writeText(schema)

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
    schemaFile.writeText(referencableSchemas)
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

    private val PARTIAL_SCHEMA =
      deserialize(
        """
        {
          "${'$'}schema": "http://json-schema.org/draft-07/schema#",
          "type": "object",
          "required": ["host", "mode", "port", "tags", "options"],
          "additionalProperties": false,
          "properties": {
            "host": {"type": "string", "pattern": "^[a-z.]+${'$'}"},
            "mode": {"type": "string", "enum": ["read", "write"]},
            "port": {"type": "integer", "minimum": 1, "maximum": 65535},
            "tags": {"type": "array", "minItems": 1, "items": {"type": "string"}},
            "options": {
              "type": "object",
              "additionalProperties": false,
              "required": ["enabled"],
              "properties": {"enabled": {"type": "boolean"}}
            }
          }
        }
        """.trimIndent(),
      )

    private val ONE_OF_SCHEMA =
      deserialize(
        """
        {
          "${'$'}schema": "http://json-schema.org/draft-07/schema#",
          "type": "object",
          "additionalProperties": false,
          "properties": {
            "credentials": {
              "type": "object",
              "oneOf": [
                {
                  "title": "API key",
                  "required": ["auth_type", "api_key"],
                  "additionalProperties": false,
                  "properties": {
                    "auth_type": {"const": "api_key"},
                    "api_key": {"type": "string", "minLength": 1}
                  }
                },
                {
                  "title": "OAuth",
                  "required": ["auth_type", "client_id", "client_secret"],
                  "additionalProperties": false,
                  "properties": {
                    "auth_type": {"const": "oauth"},
                    "client_id": {"type": "string", "minLength": 1},
                    "client_secret": {"type": "string", "minLength": 1}
                  }
                }
              ]
            }
          }
        }
        """.trimIndent(),
      )

    private val OPTIONAL_SELECTOR_ONE_OF_SCHEMA =
      deserialize(
        """
        {
          "${'$'}schema": "http://json-schema.org/draft-07/schema#",
          "type": "object",
          "properties": {
            "auth_method": {
              "type": "object",
              "oneOf": [
                {
                  "title": "ApiKeyAuth",
                  "required": ["api_key"],
                  "properties": {
                    "api_key": {"type": "string", "minLength": 1},
                    "mode": {"type": "string", "const": "api_key_auth"}
                  }
                },
                {
                  "title": "NoAuth",
                  "properties": {
                    "mode": {"type": "string", "const": "no_auth"}
                  }
                }
              ]
            }
          }
        }
        """.trimIndent(),
      )

    private val REFERENCED_OPTIONAL_SELECTOR_ONE_OF_SCHEMA =
      deserialize(
        """
        {
          "${'$'}schema": "http://json-schema.org/draft-07/schema#",
          "type": "object",
          "properties": {
            "auth_method": {
              "type": "object",
              "oneOf": [
                {"${'$'}ref": "#/definitions/api_key_auth"},
                {"${'$'}ref": "#/definitions/no_auth"}
              ]
            }
          },
          "definitions": {
            "api_key_auth": {
              "type": "object",
              "additionalProperties": false,
              "required": ["api_key"],
              "properties": {
                "api_key": {"type": "string", "minLength": 1},
                "mode": {"type": "string", "enum": ["api_key_auth"]}
              }
            },
            "no_auth": {
              "type": "object",
              "additionalProperties": false,
              "properties": {
                "mode": {"type": "string", "enum": ["no_auth"]}
              }
            }
          }
        }
        """.trimIndent(),
      )

    private val CHAINED_REFERENCED_ONE_OF_SCHEMA =
      deserialize(
        """
        {
          "${'$'}schema": "http://json-schema.org/draft-07/schema#",
          "type": "object",
          "properties": {
            "auth_method": {
              "type": "object",
              "oneOf": [
                {"${'$'}ref": "#/definitions/api_key_auth_alias"},
                {"${'$'}ref": "#/definitions/no_auth_alias"}
              ]
            }
          },
          "definitions": {
            "api_key_auth_alias": {"${'$'}ref": "#/definitions/api_key_auth"},
            "no_auth_alias": {"${'$'}ref": "#/definitions/no_auth"},
            "api_key_auth": {
              "type": "object",
              "additionalProperties": false,
              "required": ["api_key"],
              "properties": {
                "api_key": {"type": "string", "minLength": 1},
                "mode": {"type": "string", "enum": ["api_key_auth"]}
              }
            },
            "no_auth": {
              "type": "object",
              "additionalProperties": false,
              "properties": {
                "mode": {"type": "string", "enum": ["no_auth"]}
              }
            }
          }
        }
        """.trimIndent(),
      )
  }
}
