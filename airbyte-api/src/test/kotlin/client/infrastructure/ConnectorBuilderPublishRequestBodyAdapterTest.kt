/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.infrastructure

import com.fasterxml.jackson.databind.ObjectMapper
import com.squareup.moshi.adapter
import io.airbyte.api.client.model.generated.ConnectorBuilderPublishRequestBody
import io.airbyte.api.client.model.generated.DeclarativeSourceManifest
import io.airbyte.commons.resources.MoreResources
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.openapitools.client.infrastructure.Serializer
import java.util.UUID

internal class ConnectorBuilderPublishRequestBodyAdapterTest {
  @Test
  @OptIn(ExperimentalStdlibApi::class)
  internal fun testSerdeConnectorBuilderPublishRequestBody() {
    val json = MoreResources.readResource("json/requests/connector_builder_project_request.json")
    val adapter = Serializer.moshi.adapter<ConnectorBuilderPublishRequestBody>()
    val result = assertDoesNotThrow { adapter.fromJson(json) }
    assertNotNull(result)

    val manifest =
      ObjectMapper().readTree(
        """
        {
          "version": "0.30.3",
          "type": "DeclarativeSource",
          "check": {
            "type": "CheckStream",
            "stream_names": [
              "records"
            ]
          },
          "streams": [
            {
              "type": "DeclarativeStream",
              "name": "records",
              "primary_key": [],
              "schema_loader": {
                "type": "InlineSchemaLoader",
                  "schema": {
                    "type": "object",
                    "${'$'}schema": "http://json-schema.org/schema#",
                    "properties": {
                      "id": {
                      "type": "string"
                    }
                  }
                }
              },
              "retriever": {
                "type": "SimpleRetriever",
                "requester": {
                  "type": "HttpRequester",
                  "url_base": "<url_base needs to be update in order to work since port is defined only in @BeforeAll>",
                  "path": "/",
                  "http_method": "GET",
                  "request_parameters": {},
                  "request_headers": {},
                  "request_body_json": "{\${'"'}records\${'"'}:[{\${'"'}id\${'"'}:1},{\${'"'}id\${'"'}:2},{\${'"'}id\${'"'}:3}]}",
                  "authenticator": {
                    "type": "NoAuth"
                  }
                },
                "record_selector": {
                  "type": "RecordSelector",
                  "extractor": {
                    "type": "DpathExtractor",
                    "field_path": [
                      "json",
                      "records"
                    ]
                  }
                },
                "paginator": {
                  "type": "NoPagination"
                }
              }
            }
          ],
          "spec": {
            "connection_specification": {
              "${'$'}schema": "http://json-schema.org/draft-07/schema#",
              "type": "object",
              "required": [],
              "properties": {},
              "additionalProperties": true
            },
            "documentation_url": "https://example.org",
            "type": "Spec"
          }
        }
        """.trimIndent(),
      )
    val spec =
      ObjectMapper().readTree(
        """
        {
          "connectionSpecification": {
            "${'$'}schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": [],
            "properties": {},
            "additionalProperties": true
          },
          "documentationUrl": "https://example.org",
          "type": "Spec"
        }
        """.trimIndent(),
      )
    val connectorBuilderPublishRequestBody =
      ConnectorBuilderPublishRequestBody(
        workspaceId = UUID.randomUUID(),
        name = "test",
        builderProjectId = UUID.randomUUID(),
        initialDeclarativeManifest =
          DeclarativeSourceManifest(
            description = "test",
            manifest = manifest,
            spec = spec,
            version = 1L,
          ),
      )
    val jsonResult = assertDoesNotThrow { adapter.toJson(connectorBuilderPublishRequestBody) }
    assertNotNull(jsonResult)
  }
}
