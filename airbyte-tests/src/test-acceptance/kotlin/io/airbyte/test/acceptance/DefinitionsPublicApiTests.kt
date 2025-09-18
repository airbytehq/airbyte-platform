/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.acceptance

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode
import io.airbyte.api.client.generated.PublicDeclarativeSourceDefinitionsApi
import io.airbyte.api.client.generated.PublicDestinationDefinitionsApi
import io.airbyte.api.client.generated.PublicSourceDefinitionsApi
import io.airbyte.api.client.model.generated.CreateDeclarativeSourceDefinitionRequest
import io.airbyte.api.client.model.generated.CreateDefinitionRequest
import io.airbyte.api.client.model.generated.UpdateDeclarativeSourceDefinitionRequest
import io.airbyte.api.client.model.generated.UpdateDefinitionRequest
import io.airbyte.commons.json.Jsons
import io.airbyte.test.utils.AcceptanceTestUtils
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.openapitools.client.infrastructure.ClientError
import org.openapitools.client.infrastructure.ClientException
import java.util.UUID

@Tag("api")
class DefinitionsPublicApiTests {
  private val testResources = AcceptanceTestsResources()
  private val sourceDefinitionsApi =
    PublicSourceDefinitionsApi(
      basePath = AcceptanceTestUtils.getAirbyteApiUrl(),
      client = AcceptanceTestUtils.createOkHttpClient(),
    )
  private val destinationDefinitionsApi =
    PublicDestinationDefinitionsApi(
      basePath = AcceptanceTestUtils.getAirbyteApiUrl(),
      client = AcceptanceTestUtils.createOkHttpClient(),
    )
  private val declarativeSourceDefinitionsApi =
    PublicDeclarativeSourceDefinitionsApi(
      basePath = AcceptanceTestUtils.getAirbyteApiUrl(),
      client = AcceptanceTestUtils.createOkHttpClient(),
    )

  @BeforeEach
  @Throws(Exception::class)
  fun setup() {
    testResources.init()
    testResources.setup()
  }

  @AfterEach
  fun tearDown() {
    testResources.tearDown()
    testResources.end()
  }

  @Test
  fun readSourceDefinitions() {
    val res = sourceDefinitionsApi.publicListSourceDefinitions(testResources.workspaceId)
    assert(res.data.isNotEmpty())

    // check for a well-known public source
    val faker = res.data.firstOrNull { it.name == "Sample Data" }
    // TODO why don't we filter by definition ID + assertEquals the name?
    //   anyway, adding this assert as a stopgap
    assertNotNull(faker, "Expected at least one source connector to be named `Sample Data`")

    val get = sourceDefinitionsApi.publicGetSourceDefinition(testResources.workspaceId, UUID.fromString(faker!!.id))
    assertEquals(faker.dockerRepository, get.dockerRepository)
    assertEquals(faker.dockerImageTag, get.dockerImageTag)
  }

  @Test
  fun manageSourceDefinition() {
    val created =
      sourceDefinitionsApi.publicCreateSourceDefinition(
        testResources.workspaceId,
        CreateDefinitionRequest(
          name = "test source",
          dockerRepository = "airbyte/source-faker",
          dockerImageTag = "6.2.20",
        ),
      )

    val uuid = UUID.fromString(created.id)
    assertEquals(created.name, "test source")
    assertEquals(created.dockerRepository, "airbyte/source-faker")
    assertEquals(created.dockerImageTag, "6.2.20")

    val get = sourceDefinitionsApi.publicGetSourceDefinition(testResources.workspaceId, uuid)
    assertEquals(get.name, "test source")
    assertEquals(get.dockerRepository, "airbyte/source-faker")
    assertEquals(get.dockerImageTag, "6.2.20")

    val updated =
      sourceDefinitionsApi.publicUpdateSourceDefinition(
        testResources.workspaceId,
        uuid,
        UpdateDefinitionRequest(
          name = "test source updated",
          dockerImageTag = "6.2.17",
        ),
      )

    assertEquals(updated.name, "test source updated")
    assertEquals(updated.dockerImageTag, "6.2.17")

    sourceDefinitionsApi.publicDeleteSourceDefinition(testResources.workspaceId, uuid)

    val exception =
      assertThrows<ClientException> {
        sourceDefinitionsApi.publicGetSourceDefinition(testResources.workspaceId, uuid)
      }
    val err = exception.response as ClientError<*>

    assertEquals(404, exception.statusCode)
    assertEquals(404, err.statusCode)
  }

  @Test
  fun manageDestinationDefinition() {
    val created =
      destinationDefinitionsApi.publicCreateDestinationDefinition(
        testResources.workspaceId,
        CreateDefinitionRequest(
          name = "test dest",
          dockerRepository = "airbyte/destination-dev-null",
          dockerImageTag = "0.7.18",
        ),
      )

    val uuid = UUID.fromString(created.id)
    assertEquals(created.name, "test dest")
    assertEquals(created.dockerRepository, "airbyte/destination-dev-null")
    assertEquals(created.dockerImageTag, "0.7.18")

    val get = destinationDefinitionsApi.publicGetDestinationDefinition(testResources.workspaceId, uuid)
    assertEquals(get.name, "test dest")
    assertEquals(get.dockerRepository, "airbyte/destination-dev-null")
    assertEquals(get.dockerImageTag, "0.7.18")

    val updated =
      destinationDefinitionsApi.publicUpdateDestinationDefinition(
        testResources.workspaceId,
        uuid,
        UpdateDefinitionRequest(
          name = "test dest updated",
          dockerImageTag = "0.7.17",
        ),
      )

    assertEquals(updated.name, "test dest updated")
    assertEquals(updated.dockerImageTag, "0.7.17")

    destinationDefinitionsApi.publicDeleteDestinationDefinition(testResources.workspaceId, uuid)

    val exception =
      assertThrows<ClientException> {
        destinationDefinitionsApi.publicGetDestinationDefinition(testResources.workspaceId, uuid)
      }
    val err = exception.response as ClientError<*>

    assertEquals(404, exception.statusCode)
    assertEquals(404, err.statusCode)
  }

  @Test
  fun manageDeclarativeSourceDefinition() {
    val created =
      declarativeSourceDefinitionsApi.publicCreateDeclarativeSourceDefinition(
        testResources.workspaceId,
        CreateDeclarativeSourceDefinitionRequest(
          name = "test declarative source",
          manifest = Jsons.deserialize(MANIFEST),
        ),
      )

    val uuid = UUID.fromString(created.id)
    assertEquals(created.name, "test declarative source")
    assertEquals(created.version, 1)

    // Check a value in the manifest
    assertEquals("JPY", getJsonPath(created.manifest, "spec.connection_specification.properties.base.default").textValue())

    // Change a value in the manifest
    val updatedManifest = Jsons.clone(created.manifest)
    (getJsonPath(updatedManifest, "spec.connection_specification.properties.base") as ObjectNode)
      .set<JsonNode>("default", TextNode("USD"))

    val updated =
      declarativeSourceDefinitionsApi.publicUpdateDeclarativeSourceDefinition(
        testResources.workspaceId,
        uuid,
        UpdateDeclarativeSourceDefinitionRequest(
          manifest = updatedManifest,
        ),
      )

    // Ensure the changed manifest value was persisted
    assertEquals("USD", getJsonPath(updated.manifest, "spec.connection_specification.properties.base.default").textValue())
  }
}

private fun getJsonPath(
  node: JsonNode,
  path: String,
): JsonNode {
  var data = node
  for (key in path.split(".")) {
    data = data.path(key)
  }
  return data
}

// kotlin has issues with $ charaters in raw string literals.
// https://youtrack.jetbrains.com/issue/KT-2425
private const val REF = "\$ref"
private const val SCHEMA = "\$schema"
private const val MANIFEST = """
  {
        "version": "6.36.3",
        "type": "DeclarativeSource",
        "check": {
            "type": "CheckStream",
            "stream_names": [
                "Rates"
            ]
        },
        "definitions": {
            "base_requester": {
                "type": "HttpRequester",
                "url_base": "https://api.apilayer.com",
                "authenticator": {
                    "type": "ApiKeyAuthenticator",
                    "api_token": "{{ config[\"api_key\"] }}",
                    "inject_into": {
                        "type": "RequestOption",
                        "field_name": "apiKey",
                        "inject_into": "header"
                    }
                }
            },
            "streams": {
                "Rates": {
                    "type": "DeclarativeStream",
                    "name": "Rates",
                    "retriever": {
                        "type": "SimpleRetriever",
                        "requester": {
                            "$REF": "#/definitions/base_requester",
                            "path": "/exchangerates_data/latest",
                            "http_method": "GET",
                            "request_parameters": {
                                "base": "{{ config['base'] }}"
                            }
                        },
                        "record_selector": {
                            "type": "RecordSelector",
                            "extractor": {
                                "type": "DpathExtractor",
                                "field_path": []
                            }
                        }
                    },
                    "schema_loader": {
                        "type": "InlineSchemaLoader",
                        "schema": {
                            "$REF": "#/schemas/Rates"
                        }
                    }
                }
            }
        },
        "streams": [
            {
                "$REF": "#/definitions/streams/Rates"
            }
        ],
        "schemas": {
            "Rates": {
                "$SCHEMA": "http://json-schema.org/schema#",
                "additionalProperties": true,
                "properties": {
                    "base": {
                        "type": [
                            "string",
                            "null"
                        ]
                    },
                    "date": {
                        "type": [
                            "string",
                            "null"
                        ]
                    },
                    "rates": {
                        "properties": {
                            "GBP": {
                                "type": [
                                    "number",
                                    "null"
                                ]
                            },
                            "JPY": {
                                "type": [
                                    "number",
                                    "null"
                                ]
                            },
                            "USD": {
                                "type": [
                                    "number",
                                    "null"
                                ]
                            },
                            "ZWL": {
                                "type": [
                                    "number",
                                    "null"
                                ]
                            }
                        },
                        "type": [
                            "object",
                            "null"
                        ]
                    },
                    "success": {
                        "type": [
                            "boolean",
                            "null"
                        ]
                    },
                    "timestamp": {
                        "type": [
                            "number",
                            "null"
                        ]
                    }
                },
                "type": "object"
            }
        },
        "spec": {
            "connection_specification": {
                "$SCHEMA": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "required": [
                    "api_key"
                ],
                "properties": {
                    "api_key": {
                        "type": "string",
                        "order": 0,
                        "title": "API Key",
                        "airbyte_secret": true
                    },
                    "base": {
                        "type": "string",
                        "order": 1,
                        "title": "Base",
                        "default": "JPY"
                    }
                },
                "additionalProperties": true
            },
            "type": "Spec"
        },
        "metadata": {
            "autoImportSchema": {
                "Rates": true
            },
            "testedStreams": {
                "Rates": {
                    "hasRecords": true,
                    "streamHash": "69c29e3a6b1edf721071453d43108d3ffbf91f13",
                    "hasResponse": true,
                    "primaryKeysAreUnique": true,
                    "primaryKeysArePresent": true,
                    "responsesAreSuccessful": true
                }
            },
            "assist": {}
        }
    }
"""
