/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.acceptance

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertNotNull
import org.junit.jupiter.api.assertThrows
import org.openapitools.client.infrastructure.ClientException
import java.util.UUID

// @Disabled
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Tag("api")
class DefinitionsPublicApiTest {
  private val atClient = AcceptanceTestClient()

  @BeforeAll
  fun setup() {
    atClient.setup()
  }

  @AfterAll
  fun tearDownAll() {
    atClient.tearDownAll()
  }

  @AfterEach
  fun tearDown() {
    atClient.tearDown()
  }

  @Test
  fun `read SourceDefinition`() {
    val definitions = atClient.public.fetchSourceDefinitions()
    val faker = definitions.data.firstOrNull { it.name == "Sample Data" }
    assertNotNull(faker, "Expected to find source definition named 'Sample Data' (faker)")

    val fakerDefinition = atClient.public.fetchSourceDefinition(UUID.fromString(faker.id))
    assertEquals(faker, fakerDefinition)
  }

  @Test
  fun `manage SourceDefinition`() {
    // create a source definition
    val createdId = atClient.public.createSourceDefinition()
    val created = atClient.public.fetchSourceDefinition(createdId)
    assertEquals(createdId, UUID.fromString(created.id))
    assertTrue(created.name.startsWith(NAME_PREFIX), "source definition name should start with $NAME_PREFIX")
    assertEquals(CUSTOM_DOCKER_REPO, created.dockerRepository)
    assertEquals(CUSTOM_DOCKER_TAG, created.dockerImageTag)

    // update it
    val updatedId = atClient.public.updateSourceDefinition(createdId)
    assertEquals(updatedId, createdId)
    val updated = atClient.public.fetchSourceDefinition(updatedId)
    assertTrue(updated.name.startsWith("$NAME_PREFIX-update"), "source definition name should start with $NAME_PREFIX-update")
    assertEquals(created.dockerRepository, updated.dockerRepository)
    assertEquals(created.dockerImageTag, updated.dockerImageTag)

    // delete it
    val deletedId = atClient.public.deleteSourceDefinition(updatedId)
    assertEquals(updatedId, deletedId)

    // verify it no longer exists
    val e = assertThrows<ClientException> { atClient.public.fetchSourceDefinition(deletedId) }
    assertEquals(404, e.statusCode)
  }

  @Test
  fun `manage DestinationDefinition`() {
    // create a destination definition
    val createdId = atClient.public.createDestinationDefinition()
    val created = atClient.public.fetchDestinationDefinition(createdId)
    assertEquals(createdId, UUID.fromString(created.id))
    assertTrue(created.name.startsWith(NAME_PREFIX), "destination definition name should start with $NAME_PREFIX")
    assertEquals(CUSTOM_DOCKER_REPO, created.dockerRepository)
    assertEquals(CUSTOM_DOCKER_TAG, created.dockerImageTag)

    // update it
    val updatedId = atClient.public.updateDestinationDefinition(createdId)
    assertEquals(updatedId, createdId)
    val updated = atClient.public.fetchDestinationDefinition(updatedId)
    assertTrue(updated.name.startsWith("$NAME_PREFIX-update"), "destination definition name should start with $NAME_PREFIX-update")
    assertEquals(created.dockerRepository, updated.dockerRepository)
    assertEquals(created.dockerImageTag, updated.dockerImageTag)

    // delete it
    val deletedId = atClient.public.deleteDestinationDefinition(updatedId)
    assertEquals(updatedId, deletedId)

    // verify it no longer exists
    val e = assertThrows<ClientException> { atClient.public.fetchDestinationDefinition(deletedId) }
    assertEquals(404, e.statusCode)
  }

  @Test
  fun `manage DeclarativeSourceDefinition`() {
    // create a declarative source definition
    val createdId = atClient.public.createDeclarativeSourceDefinition(manifest())
    val created = atClient.public.fetchDeclarativeSourceDefinition(createdId)

    assertTrue(created.name.startsWith(NAME_PREFIX), "declarative source definition name should start with $NAME_PREFIX")
    assertEquals(1, created.version)

    val monetaryValue =
      created.manifest
        .at("/spec/connection_specification/properties/base/default")
        .takeIf { !it.isMissingNode }
        ?.textValue()
    assertEquals("JPY", monetaryValue)

    // update it
    val updatedId =
      atClient.public.updateDeclarativeSourceDefinition(
        decSrcDefId = createdId,
        manifest = manifest("USD"),
      )
    assertEquals(updatedId, createdId)

    val updated = atClient.public.fetchDeclarativeSourceDefinition(updatedId)
    assertEquals(1, created.version)

    val updatedMonetaryValue =
      updated.manifest
        .at("/spec/connection_specification/properties/base/default")
        .takeIf { !it.isMissingNode }
        ?.textValue()
    assertEquals("USD", updatedMonetaryValue)
  }
}

/**
 * Generates a JSON manifest describing the configuration and structure of a declarative data source.
 *
 * @param defaultMonetaryValue Default value for the "base" monetary unit in the specification, defaults to "JPY".
 * @return A JSON string representing the manifest configuration for the data source.
 */
private fun manifest(defaultMonetaryValue: String = "JPY"): String =
  $$"""
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
                        "$ref": "#/definitions/base_requester",
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
                        "$ref": "#/schemas/Rates"
                    }
                }
            }
        }
    },
    "streams": [
        {
            "$ref": "#/definitions/streams/Rates"
        }
    ],
    "schemas": {
        "Rates": {
            "$schema": "http://json-schema.org/schema#",
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
            "$schema": "http://json-schema.org/draft-07/schema#",
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
                    "default": "$$defaultMonetaryValue"
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
