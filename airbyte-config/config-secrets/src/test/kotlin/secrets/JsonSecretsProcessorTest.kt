/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.collect.ImmutableMap
import io.airbyte.commons.constants.AirbyteSecretConstants
import io.airbyte.commons.json.Jsons
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.io.IOException
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
    private const val S3_BUCKET_NAME = "s3_bucket_name"
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
    private fun scenarioProvider(): Stream<Arguments?>? {
      return Stream.of<Arguments?>(
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
    }

    @JvmStatic
    private fun scenarioProviderNoOp(): Stream<Arguments> {
      return Stream.of<Arguments>(
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
  }

  private lateinit var processor: JsonSecretsProcessor

  @BeforeEach
  fun setup() {
    processor = JsonSecretsProcessor(true)
  }

  @Test
  fun testCopySecrets() {
    val src =
      Jsons.jsonNode<ImmutableMap<Any, Any>>(
        ImmutableMap.builder<Any, Any>()
          .put(
            FIELD_1,
            VALUE_1,
          )
          .put(FIELD_2, 2)
          .put(
            ADDITIONAL_FIELD,
            DONT_COPY_ME,
          )
          .put(
            SECRET_1,
            DONT_TELL_ANYONE,
          )
          .put(SECRET_2, "updateme")
          .build(),
      )
    val dst =
      Jsons.jsonNode<ImmutableMap<Any, Any>>(
        ImmutableMap.builder<Any, Any>()
          .put(
            FIELD_1,
            VALUE_1,
          )
          .put(FIELD_2, 2)
          .put(SECRET_1, AirbyteSecretConstants.SECRETS_MASK)
          .put(SECRET_2, "newvalue")
          .build(),
      )
    val actual = processor.copySecrets(src, dst, SCHEMA_ONE_LAYER)
    val expected =
      Jsons.jsonNode<ImmutableMap<Any, Any>>(
        ImmutableMap.builder<Any, Any>()
          .put(
            FIELD_1,
            VALUE_1,
          )
          .put(FIELD_2, 2)
          .put(
            SECRET_1,
            DONT_TELL_ANYONE,
          )
          .put(SECRET_2, "newvalue")
          .build(),
      )
    Assertions.assertEquals(expected, actual)
  }

  @Test
  fun testCopySecretsNotInSrc() {
    val src =
      Jsons.jsonNode<ImmutableMap<Any, Any>>(
        ImmutableMap.builder<Any, Any>()
          .put(
            FIELD_1,
            VALUE_1,
          )
          .put(FIELD_2, 2)
          .put(
            ADDITIONAL_FIELD,
            DONT_COPY_ME,
          )
          .build(),
      )
    val dst =
      Jsons.jsonNode<ImmutableMap<Any, Any>>(
        ImmutableMap.builder<Any, Any>()
          .put(
            FIELD_1,
            VALUE_1,
          )
          .put(FIELD_2, 2)
          .put(SECRET_1, AirbyteSecretConstants.SECRETS_MASK)
          .build(),
      )
    val expected = dst.deepCopy<JsonNode>()
    val actual = processor.copySecrets(src, dst, SCHEMA_ONE_LAYER)
    Assertions.assertEquals(expected, actual)
  }

  @Test
  fun testCopySecretInnerObject() {
    val srcOneOf =
      Jsons.jsonNode<ImmutableMap<Any, Any>>(
        ImmutableMap.builder<Any, Any>()
          .put(
            S3_BUCKET_NAME,
            NAME,
          )
          .put(SECRET_ACCESS_KEY, "secret")
          .put(
            ADDITIONAL_FIELD,
            DONT_COPY_ME,
          )
          .build(),
      )
    val src =
      Jsons.jsonNode<ImmutableMap<Any, Any>>(
        ImmutableMap.builder<Any, Any>()
          .put(
            WAREHOUSE,
            HOUSE,
          )
          .put("loading_method", srcOneOf).build(),
      )
    val dstOneOf =
      Jsons.jsonNode<ImmutableMap<Any, Any>>(
        ImmutableMap.builder<Any, Any>()
          .put(
            S3_BUCKET_NAME,
            NAME,
          )
          .put(SECRET_ACCESS_KEY, AirbyteSecretConstants.SECRETS_MASK)
          .build(),
      )
    val dst =
      Jsons.jsonNode<ImmutableMap<Any, Any>>(
        ImmutableMap.builder<Any, Any>()
          .put(
            WAREHOUSE,
            HOUSE,
          )
          .put(LOADING_METHOD, dstOneOf).build(),
      )
    val actual = processor.copySecrets(src, dst, SCHEMA_INNER_OBJECT)
    val expectedOneOf =
      Jsons.jsonNode<ImmutableMap<Any, Any>>(
        ImmutableMap.builder<Any, Any>()
          .put(
            S3_BUCKET_NAME,
            NAME,
          )
          .put(SECRET_ACCESS_KEY, "secret").build(),
      )
    val expected =
      Jsons.jsonNode<ImmutableMap<Any, Any>>(
        ImmutableMap.builder<Any, Any>()
          .put(
            WAREHOUSE,
            HOUSE,
          )
          .put(LOADING_METHOD, expectedOneOf).build(),
      )
    Assertions.assertEquals(expected, actual)
  }

  @Test
  fun testCopySecretNotInSrcInnerObject() {
    val src =
      Jsons.jsonNode<ImmutableMap<Any, Any>>(
        ImmutableMap.builder<Any, Any>()
          .put(
            WAREHOUSE,
            HOUSE,
          ).build(),
      )
    val dstOneOf =
      Jsons.jsonNode<ImmutableMap<Any, Any>>(
        ImmutableMap.builder<Any, Any>()
          .put(
            S3_BUCKET_NAME,
            NAME,
          )
          .put(SECRET_ACCESS_KEY, AirbyteSecretConstants.SECRETS_MASK)
          .build(),
      )
    val dst =
      Jsons.jsonNode<ImmutableMap<Any, Any>>(
        ImmutableMap.builder<Any, Any>()
          .put(
            WAREHOUSE,
            HOUSE,
          )
          .put(LOADING_METHOD, dstOneOf).build(),
      )
    val actual = processor.copySecrets(src, dst, SCHEMA_INNER_OBJECT)
    val expected = dst.deepCopy<JsonNode>()
    Assertions.assertEquals(expected, actual)
  }

  // test the case where multiple sub schemas of a oneOf contain the same key but a different type.
  @Test
  fun testHandlesSameKeyInOneOf() {
    val compressionCodecObject =
      Jsons.jsonNode(
        ImmutableMap.of(
          "codec",
          "no compression",
        ),
      )
    val avroConfig =
      Jsons.jsonNode(
        ImmutableMap.of(
          "format_type",
          "Avro",
          "compression_codec",
          compressionCodecObject,
        ),
      )
    val src =
      Jsons.jsonNode(
        ImmutableMap.of(
          "client_id",
          "whatever",
          "format",
          avroConfig,
        ),
      )
    val parquetConfig =
      Jsons.jsonNode(
        ImmutableMap.of(
          "format_type",
          "Parquet",
          "compression_codec",
          "GZIP",
        ),
      )
    val dst =
      Jsons.jsonNode(
        ImmutableMap.of(
          "client_id",
          "whatever",
          "format",
          parquetConfig,
        ),
      )
    processor.copySecrets(src, dst, ONE_OF_WITH_SAME_KEY_IN_SUB_SCHEMAS)
  }

  @ParameterizedTest
  @MethodSource("scenarioProvider")
  @Throws(IOException::class)
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
    Assertions.assertEquals(expected, actual)
  }

  @Test
  @Throws(JsonProcessingException::class)
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
    Assertions.assertEquals(expected, copied)
  }

  @Test
  @Throws(JsonProcessingException::class)
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
    Assertions.assertEquals(expected, copied)
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
    Assertions.assertEquals(expected, result)
  }

  @Test
  fun testCopySecretsNoOp() {
    processor = JsonSecretsProcessor(false)

    val src =
      Jsons.jsonNode<ImmutableMap<Any, Any>>(
        ImmutableMap.builder<Any, Any>()
          .put(
            FIELD_1,
            VALUE_1,
          )
          .put(FIELD_2, 2)
          .put(
            ADDITIONAL_FIELD,
            DONT_COPY_ME,
          )
          .put(
            SECRET_1,
            DONT_TELL_ANYONE,
          )
          .put(SECRET_2, "updateme")
          .build(),
      )
    val dst =
      Jsons.jsonNode<ImmutableMap<Any, Any>>(
        ImmutableMap.builder<Any, Any>()
          .put(
            FIELD_1,
            VALUE_1,
          )
          .put(FIELD_2, 2)
          .put(SECRET_1, AirbyteSecretConstants.SECRETS_MASK)
          .put(SECRET_2, "newvalue")
          .build(),
      )
    val actual = processor.copySecrets(src, dst, SCHEMA_ONE_LAYER)
    val expected =
      Jsons.jsonNode<ImmutableMap<Any, Any>>(
        ImmutableMap.builder<Any, Any>()
          .put(
            FIELD_1,
            VALUE_1,
          )
          .put(FIELD_2, 2)
          .put(
            ADDITIONAL_FIELD,
            DONT_COPY_ME,
          )
          .put(
            SECRET_1,
            DONT_TELL_ANYONE,
          )
          .put(SECRET_2, "updateme")
          .build(),
      )
    Assertions.assertEquals(expected, actual)
  }

  @ParameterizedTest
  @MethodSource("scenarioProviderNoOp")
  @Throws(IOException::class)
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
    Assertions.assertEquals(expected, actual)
  }
}
