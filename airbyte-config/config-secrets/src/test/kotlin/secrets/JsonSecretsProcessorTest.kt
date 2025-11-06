/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.commons.constants.AirbyteSecretConstants
import io.airbyte.commons.constants.AirbyteSecretConstants.AIRBYTE_SECRET_COORDINATE_PREFIX
import io.airbyte.commons.json.Jsons
import io.airbyte.domain.models.SecretStorage
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.UUID
import java.util.stream.Stream

internal class JsonSecretsProcessorTest {
  companion object {
    private val SCHEMA_ONE_LAYER =
      Jsons.deserialize(
        """
        {
          "type": "object",  "properties": {
            "secret1": {
              "type": "string",
              "airbyte_secret": true
            },
            "secret2": {
              "type": "string",
              "airbyte_secret": "true"
            },
            "field1": {
              "type": "string"
            },
            "field2": {
              "type": "number"
            }
          }
        }
        
        """.trimIndent(),
      )

    private val SCHEMA_INNER_OBJECT =
      Jsons.deserialize(
        """
        {
            "type": "object",
            "properties": {
              "warehouse": {
                "type": "string"
              },
              "loading_method": {
                "type": "object",
                "oneOf": [
                  {
                    "properties": {}
                  },
                  {
                    "properties": {
                      "s3_bucket_name": {
                        "type": "string"
                      },
                      "secret_access_key": {
                        "type": "string",
                        "airbyte_secret": true
                      }
                    }
                  }
                ]
              }
            }
          }
        """.trimIndent(),
      )

    private val ONE_OF_WITH_SAME_KEY_IN_SUB_SCHEMAS =
      Jsons.deserialize(
        """
        {
            "${'$'}schema": "http://json-schema.org/draft-07/schema#",
            "title": "S3 Destination Spec",
            "type": "object",
            "required": [
              "client_id",
              "format"
            ],
            "additionalProperties": false,
            "properties": {
              "client_id": {
                "title": "client it",
                "type": "string",
                "default": ""
              },
              "format": {
                "title": "Output Format",
                "type": "object",
                "description": "Output data format",
                "oneOf": [
                  {
                    "title": "Avro: Apache Avro",
                    "required": ["format_type", "compression_codec"],
                    "properties": {
                      "format_type": {
                        "type": "string",
                        "enum": ["Avro"],
                        "default": "Avro"
                      },
                      "compression_codec": {
                        "title": "Compression Codec",
                        "description": "The compression algorithm used to compress data. Default to no compression.",
                        "type": "object",
                        "oneOf": [
                          {
                            "title": "no compression",
                            "required": ["codec"],
                            "properties": {
                              "codec": {
                                "type": "string",
                                "enum": ["no compression"],
                                "default": "no compression"
                              }
                            }
                          },
                          {
                            "title": "Deflate",
                            "required": ["codec", "compression_level"],
                            "properties": {
                              "codec": {
                                "type": "string",
                                "enum": ["Deflate"],
                                "default": "Deflate"
                              },
                              "compression_level": {
                                "type": "integer",
                                "default": 0,
                                "minimum": 0,
                                "maximum": 9
                              }
                            }
                          }
                        ]
                      }
                    }
                  },
                  {
                    "title": "Parquet: Columnar Storage",
                    "required": ["format_type"],
                    "properties": {
                      "format_type": {
                        "type": "string",
                        "enum": ["Parquet"],
                        "default": "Parquet"
                      },
                      "compression_codec": {
                        "type": "string",
                        "enum": [
                          "UNCOMPRESSED",
                          "GZIP"
                        ],
                        "default": "UNCOMPRESSED"
                      }
                    }
                  }
                ]
              }
            }
          }
        """.trimIndent(),
      )

    private const val FIELD_1 = "field1"
    private const val VALUE_1 = "value1"
    private const val FIELD_2 = "field2"
    private const val ADDITIONAL_FIELD = "additional_field"
    private const val DONT_COPY_ME = "dont_copy_me"
    private const val DONT_TELL_ANYONE = "donttellanyone"
    private const val SECRET_1 = "secret1"
    private const val SECRET_2 = "secret2"
    private const val NAME = "name"
    private const val BUCKET_NAME = "bucket_name"
    private const val SECRET_ACCESS_KEY = "secret_access_key"
    private const val HOUSE = "house"
    private const val WAREHOUSE = "warehouse"
    private const val LOADING_METHOD = "loading_method"
    private const val ARRAY = "array"
    private const val ARRAY_OF_ONEOF = "array_of_oneof"
    private const val NESTED_OBJECT = "nested_object"
    private const val NESTED_ONEOF = "nested_oneof"
    private const val ONE_OF_SECRET = "oneof_secret"
    private const val ONE_OF = "oneof"
    private const val OPTIONAL_PASSWORD = "optional_password"
    private const val POSTGRES_SSH_KEY = "postgres_ssh_key"
    private const val SIMPLE = "simple"

    @JvmStatic
    private fun scenarioProvider(): Stream<Arguments?>? =
      Stream.of<Arguments?>(
        Arguments.of(ARRAY, true),
        Arguments.of(ARRAY, false),
        Arguments.of(ARRAY_OF_ONEOF, true),
        Arguments.of(ARRAY_OF_ONEOF, false),
        Arguments.of(NESTED_OBJECT, true),
        Arguments.of(NESTED_OBJECT, false),
        Arguments.of(NESTED_ONEOF, true),
        Arguments.of(NESTED_ONEOF, false),
        Arguments.of(ONE_OF, true),
        Arguments.of(ONE_OF, false),
        Arguments.of(ONE_OF_SECRET, true),
        Arguments.of(ONE_OF_SECRET, false),
        Arguments.of(OPTIONAL_PASSWORD, true),
        Arguments.of(OPTIONAL_PASSWORD, false),
        Arguments.of(POSTGRES_SSH_KEY, true),
        Arguments.of(POSTGRES_SSH_KEY, false),
        Arguments.of(SIMPLE, true),
        Arguments.of(SIMPLE, false),
        Arguments.of("enum", false),
      )

    @JvmStatic
    private fun scenarioProviderNoOp(): Stream<Arguments> =
      Stream.of(
        Arguments.of(ARRAY, true),
        Arguments.of(ARRAY, false),
        Arguments.of(ARRAY_OF_ONEOF, true),
        Arguments.of(ARRAY_OF_ONEOF, false),
        Arguments.of(NESTED_OBJECT, true),
        Arguments.of(NESTED_OBJECT, false),
        Arguments.of(NESTED_ONEOF, true),
        Arguments.of(NESTED_ONEOF, false),
        Arguments.of(ONE_OF, true),
        Arguments.of(ONE_OF, false),
        Arguments.of(OPTIONAL_PASSWORD, true),
        Arguments.of(OPTIONAL_PASSWORD, false),
        Arguments.of(POSTGRES_SSH_KEY, true),
        Arguments.of(POSTGRES_SSH_KEY, false),
        Arguments.of(SIMPLE, true),
        Arguments.of(SIMPLE, false),
      )
  }

  private lateinit var processor: JsonSecretsProcessor

  @BeforeEach
  fun setup() {
    processor = JsonSecretsProcessor(true)
  }

  @Test
  fun testCopySecrets() {
    val src =
      Jsons.jsonNode(
        mapOf(
          FIELD_1 to VALUE_1,
          FIELD_2 to 2,
          ADDITIONAL_FIELD to DONT_COPY_ME,
          SECRET_1 to DONT_TELL_ANYONE,
          SECRET_2 to "updateme",
        ),
      )
    val dst =
      Jsons.jsonNode(
        mapOf(
          FIELD_1 to VALUE_1,
          FIELD_2 to 2,
          SECRET_1 to AirbyteSecretConstants.SECRETS_MASK,
          SECRET_2 to "newvalue",
        ),
      )
    val actual = processor.copySecrets(src, dst, SCHEMA_ONE_LAYER)
    val expected =
      Jsons.jsonNode(
        mapOf(
          FIELD_1 to VALUE_1,
          FIELD_2 to 2,
          SECRET_1 to DONT_TELL_ANYONE,
          SECRET_2 to "newvalue",
        ),
      )
    assertEquals(expected, actual)
  }

  @Test
  fun testCopySecretsNotInSrc() {
    val src =
      Jsons.jsonNode(
        mapOf(
          FIELD_1 to VALUE_1,
          FIELD_2 to 2,
          ADDITIONAL_FIELD to DONT_COPY_ME,
        ),
      )
    val dst =
      Jsons.jsonNode(
        mapOf(
          FIELD_1 to VALUE_1,
          FIELD_2 to 2,
          SECRET_1 to AirbyteSecretConstants.SECRETS_MASK,
        ),
      )
    val expected = dst.deepCopy<JsonNode>()
    val actual = processor.copySecrets(src, dst, SCHEMA_ONE_LAYER)
    assertEquals(expected, actual)
  }

  @Test
  fun testCopySecretInnerObject() {
    val srcOneOf =
      Jsons.jsonNode(
        mapOf(
          BUCKET_NAME to NAME,
          SECRET_ACCESS_KEY to "secret",
          ADDITIONAL_FIELD to DONT_COPY_ME,
        ),
      )
    val src =
      Jsons.jsonNode(
        mapOf(
          WAREHOUSE to HOUSE,
          "loading_method" to srcOneOf,
        ),
      )
    val dstOneOf =
      Jsons.jsonNode(
        mapOf(
          BUCKET_NAME to NAME,
          SECRET_ACCESS_KEY to AirbyteSecretConstants.SECRETS_MASK,
        ),
      )
    val dst =
      Jsons.jsonNode(
        mapOf(
          WAREHOUSE to HOUSE,
          LOADING_METHOD to dstOneOf,
        ),
      )
    val actual = processor.copySecrets(src, dst, SCHEMA_INNER_OBJECT)
    val expectedOneOf =
      Jsons.jsonNode(
        mapOf(
          BUCKET_NAME to NAME,
          SECRET_ACCESS_KEY to "secret",
        ),
      )
    val expected =
      Jsons.jsonNode(
        mapOf(
          WAREHOUSE to HOUSE,
          LOADING_METHOD to expectedOneOf,
        ),
      )
    assertEquals(expected, actual)
  }

  @Test
  fun testCopySecretNotInSrcInnerObject() {
    val src =
      Jsons.jsonNode(
        mapOf(
          WAREHOUSE to HOUSE,
        ),
      )
    val dstOneOf =
      Jsons.jsonNode(
        mapOf(
          BUCKET_NAME to NAME,
          SECRET_ACCESS_KEY to AirbyteSecretConstants.SECRETS_MASK,
        ),
      )
    val dst =
      Jsons.jsonNode(
        mapOf(
          WAREHOUSE to HOUSE,
          LOADING_METHOD to dstOneOf,
        ),
      )
    val actual = processor.copySecrets(src, dst, SCHEMA_INNER_OBJECT)
    val expected = dst.deepCopy<JsonNode>()
    assertEquals(expected, actual)
  }

  // test the case where multiple sub schemas of a oneOf contain the same key but a different type.
  @Test
  fun testHandlesSameKeyInOneOf() {
    val compressionCodecObject =
      Jsons.jsonNode(
        mapOf(
          "codec" to "no compression",
        ),
      )
    val avroConfig =
      Jsons.jsonNode(
        mapOf(
          "format_type" to "Avro",
          "compression_codec" to compressionCodecObject,
        ),
      )
    val src =
      Jsons.jsonNode(
        mapOf(
          "client_id" to "whatever",
          "format" to avroConfig,
        ),
      )
    val parquetConfig =
      Jsons.jsonNode(
        mapOf(
          "format_type" to "Parquet",
          "compression_codec" to "GZIP",
        ),
      )
    val dst =
      Jsons.jsonNode(
        mapOf(
          "client_id" to "whatever",
          "format" to parquetConfig,
        ),
      )
    processor.copySecrets(src, dst, ONE_OF_WITH_SAME_KEY_IN_SUB_SCHEMAS)
  }

  @ParameterizedTest
  @MethodSource("scenarioProvider")
  fun testSecretScenario(
    folder: String,
    partial: Boolean,
  ) {
    val objectMapper = ObjectMapper()

    val specIs = javaClass.classLoader.getResourceAsStream("$folder/spec.json")
    val specs = objectMapper.readTree(specIs)

    val inputFilePath = folder + if (partial) "/partial_config.json" else "/full_config.json"
    val inputIs = javaClass.classLoader.getResourceAsStream(inputFilePath)
    val input = objectMapper.readTree(inputIs)

    val expectedFilePath = "$folder/expected.json"
    val expectedIs = javaClass.classLoader.getResourceAsStream(expectedFilePath)
    val expected = objectMapper.readTree(expectedIs)

    val actual = processor.prepareSecretsForOutput(input, specs)
    assertEquals(expected, actual)
  }

  @Test
  fun copiesSecretsInNestedNonCombinationNode() {
    val objectMapper = ObjectMapper()
    val source =
      objectMapper.readTree(
        """
        {
          "top_level": {
            "a_secret": "hunter2"
          }
        }
        
        """.trimIndent(),
      )
    val dest =
      objectMapper.readTree(
        """
        {
          "top_level": {
            "a_secret": "**********"
          }
        }
        
        """.trimIndent(),
      )
    val schema =
      objectMapper.readTree(
        """
        {
          "type": "object",
          "properties": {
            "top_level": {
              "type": "object",
              "properties": {
                "a_secret": {
                  "type": "string",
                  "airbyte_secret": true
                }
              }
            }
          }
        }
        
        """.trimIndent(),
      )
    val copied = processor.copySecrets(source, dest, schema)
    val expected =
      objectMapper.readTree(
        """
        {
          "top_level": {
            "a_secret": "hunter2"
          }
        }
        
        """.trimIndent(),
      )
    assertEquals(expected, copied)
  }

  @Test
  fun doesNotCopySecretsInNestedNonCombinationNodeWhenDestinationMissing() {
    val objectMapper = ObjectMapper()
    val source =
      objectMapper.readTree(
        """
        {
          "top_level": {
            "a_secret": "hunter2"
          }
        }
        
        """.trimIndent(),
      )
    val dest =
      objectMapper.readTree(
        """
        {
          "top_level": {
          }
        }
        
        """.trimIndent(),
      )
    val schema =
      objectMapper.readTree(
        """
        {
          "type": "object",
          "properties": {
            "top_level": {
              "type": "object",
              "properties": {
                "a_secret": {
                  "type": "string",
                  "airbyte_secret": true
                }
              }
            }
          }
        }
        
        """.trimIndent(),
      )
    val copied = processor.copySecrets(source, dest, schema)
    val expected =
      objectMapper.readTree(
        """
        {
          "top_level": {
          }
        }
        
        """.trimIndent(),
      )
    assertEquals(expected, copied)
  }

  @Test
  fun testCopySecretsWithTopLevelOneOf() {
    val schema =
      Jsons.deserialize(
        """
        {
            "${'$'}schema": "http://json-schema.org/draft-07/schema#",
            "title": "E2E Test Destination Spec",
            "type": "object",
            "oneOf": [
              {
                "title": "Silent",
                "required": ["type"],
                "properties": {
                  "a_secret": {
                    "type": "string",
                    "airbyte_secret": true
                  }
                }
              },
              {
                "title": "Throttled",
                "required": ["type", "millis_per_record"],
                "properties": {
                  "type": {
                    "type": "string",
                    "const": "THROTTLED",
                    "default": "THROTTLED"
                  },
                  "millis_per_record": {
                    "description": "Number of milli-second to pause in between records.",
                    "type": "integer"
                  }
                }
              }
            ]
          }
        
        """.trimIndent(),
      )
    val source =
      Jsons.deserialize(
        """
        {
          "type": "THROTTLED",
          "a_secret": "woot"
        }
        
        """.trimIndent(),
      )
    val destination =
      Jsons.deserialize(
        """
        {
          "type": "THROTTLED",
          "a_secret": "**********"
        }
        
        """.trimIndent(),
      )
    val result = processor.copySecrets(source, destination, schema)
    val expected =
      Jsons.deserialize(
        """
        {
          "type": "THROTTLED",
          "a_secret": "woot"
        }
        
        """.trimIndent(),
      )
    assertEquals(expected, result)
  }

  @Test
  fun testCopySecretsNoOp() {
    processor = JsonSecretsProcessor(false)

    val src =
      Jsons.jsonNode(
        mapOf(
          FIELD_1 to VALUE_1,
          FIELD_2 to 2,
          ADDITIONAL_FIELD to DONT_COPY_ME,
          SECRET_1 to DONT_TELL_ANYONE,
          SECRET_2 to "updateme",
        ),
      )
    val dst =
      Jsons.jsonNode(
        mapOf(
          FIELD_1 to VALUE_1,
          FIELD_2 to 2,
          SECRET_1 to AirbyteSecretConstants.SECRETS_MASK,
          SECRET_2 to "newvalue",
        ),
      )
    val actual = processor.copySecrets(src, dst, SCHEMA_ONE_LAYER)
    val expected =
      Jsons.jsonNode(
        mapOf(
          FIELD_1 to VALUE_1,
          FIELD_2 to 2,
          ADDITIONAL_FIELD to DONT_COPY_ME,
          SECRET_1 to DONT_TELL_ANYONE,
          SECRET_2 to "updateme",
        ),
      )
    assertEquals(expected, actual)
  }

  @ParameterizedTest
  @MethodSource("scenarioProviderNoOp")
  fun testSecretScenarioNoOp(
    folder: String,
    partial: Boolean,
  ) {
    processor = JsonSecretsProcessor(false)

    val objectMapper = ObjectMapper()

    val specIs = javaClass.classLoader.getResourceAsStream("$folder/spec.json")
    val specs = objectMapper.readTree(specIs)

    val inputFilePath = folder + if (partial) "/partial_config.json" else "/full_config.json"
    val inputIs = javaClass.classLoader.getResourceAsStream(inputFilePath)
    val input = objectMapper.readTree(inputIs)

    val expectedFilePath = "$folder/expected.json"
    val expectedIs = javaClass.classLoader.getResourceAsStream(expectedFilePath)
    val expected = objectMapper.readTree(expectedIs)

    val actual = processor.prepareSecretsForOutput(input, specs)
    assertEquals(expected, actual)
  }

  @Test
  fun `test simplify secrets with default secret storage is default and showing coordinates from the default manager is disabled`() {
    val secretCoordinate = "external_secret_123abc"
    val secretPayload =
      Jsons.jsonNode(
        mapOf(
          "_secret" to secretCoordinate,
        ),
      )
    val src =
      Jsons.jsonNode(
        mapOf(
          FIELD_1 to VALUE_1,
          FIELD_2 to 2,
          SECRET_1 to secretPayload,
        ),
      )

    val secretReferenceConfig =
      SecretReferenceConfig(
        SecretCoordinate.ExternalSecretCoordinate(secretCoordinate),
        SecretStorage.DEFAULT_SECRET_STORAGE_ID.value,
      )
    val referencedSecrets = mapOf("$.$SECRET_1" to secretReferenceConfig)
    val configWIthSecretReferences =
      ConfigWithSecretReferences(
        src,
        referencedSecrets,
      )
    val actual = processor.simplifySecretsForOutput(configWIthSecretReferences, SCHEMA_ONE_LAYER, false)
    val expected =
      Jsons.jsonNode(
        mapOf(
          FIELD_1 to VALUE_1,
          FIELD_2 to 2,
          SECRET_1 to "**********",
        ),
      )
    assertEquals(expected, actual)
  }

  @Test
  fun `test simplify secrets with non-default secret storage and showing coordinates from the default manager is disabled`() {
    val secretCoordinate = "external_secret_123abc"
    val secretPayload =
      Jsons.jsonNode(
        mapOf(
          "_secret" to secretCoordinate,
        ),
      )
    val src =
      Jsons.jsonNode(
        mapOf(
          FIELD_1 to VALUE_1,
          FIELD_2 to 2,
          SECRET_1 to secretPayload,
        ),
      )

    val secretReferenceConfig =
      SecretReferenceConfig(
        SecretCoordinate.ExternalSecretCoordinate(secretCoordinate),
        UUID.randomUUID(),
      )
    val referencedSecrets = mapOf("$.$SECRET_1" to secretReferenceConfig)
    val configWIthSecretReferences =
      ConfigWithSecretReferences(
        src,
        referencedSecrets,
      )
    val actual = processor.simplifySecretsForOutput(configWIthSecretReferences, SCHEMA_ONE_LAYER, false)
    val expected =
      Jsons.jsonNode(
        mapOf(
          FIELD_1 to VALUE_1,
          FIELD_2 to 2,
          SECRET_1 to "${AIRBYTE_SECRET_COORDINATE_PREFIX}$secretCoordinate",
        ),
      )
    assertEquals(expected, actual)
  }

  @Test
  fun `test simplify secrets for output with default secret storage and showing coordinates from the default manager is enabled`() {
    val secretCoordinate = "external_secret_123abc"
    val secretPayload =
      Jsons.jsonNode(
        mapOf(
          "_secret" to secretCoordinate,
        ),
      )
    val src =
      Jsons.jsonNode(
        mapOf(
          FIELD_1 to VALUE_1,
          FIELD_2 to 2,
          SECRET_1 to secretPayload,
        ),
      )

    val secretReferenceConfig =
      SecretReferenceConfig(
        SecretCoordinate.ExternalSecretCoordinate(secretCoordinate),
        SecretStorage.DEFAULT_SECRET_STORAGE_ID.value,
      )
    val referencedSecrets = mapOf("$.$SECRET_1" to secretReferenceConfig)
    val configWIthSecretReferences =
      ConfigWithSecretReferences(
        src,
        referencedSecrets,
      )
    val actual = processor.simplifySecretsForOutput(configWIthSecretReferences, SCHEMA_ONE_LAYER, true)
    val expected =
      Jsons.jsonNode(
        mapOf(
          FIELD_1 to VALUE_1,
          FIELD_2 to 2,
          SECRET_1 to "${AIRBYTE_SECRET_COORDINATE_PREFIX}$secretCoordinate",
        ),
      )
    assertEquals(expected, actual)
  }

  @Test
  fun `test simplify secrets for output exposes with non default storage and showing coordinates from the default manager is enabled`() {
    val secretCoordinate = "external_secret_123abc"
    val secretPayload =
      Jsons.jsonNode(
        mapOf(
          "_secret" to secretCoordinate,
        ),
      )
    val src =
      Jsons.jsonNode(
        mapOf(
          FIELD_1 to VALUE_1,
          FIELD_2 to 2,
          SECRET_1 to secretPayload,
        ),
      )

    val secretReferenceConfig =
      SecretReferenceConfig(
        SecretCoordinate.ExternalSecretCoordinate(secretCoordinate),
        UUID.randomUUID(),
      )
    val referencedSecrets = mapOf("$.$SECRET_1" to secretReferenceConfig)
    val configWIthSecretReferences =
      ConfigWithSecretReferences(
        src,
        referencedSecrets,
      )
    val actual = processor.simplifySecretsForOutput(configWIthSecretReferences, SCHEMA_ONE_LAYER, true)
    val expected =
      Jsons.jsonNode(
        mapOf(
          FIELD_1 to VALUE_1,
          FIELD_2 to 2,
          SECRET_1 to "${AIRBYTE_SECRET_COORDINATE_PREFIX}$secretCoordinate",
        ),
      )
    assertEquals(expected, actual)
  }
}
