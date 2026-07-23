/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.acceptance

import io.airbyte.api.client.model.generated.ConnectionCreate
import io.airbyte.api.client.model.generated.ConnectionStatus
import io.airbyte.api.client.model.generated.JobStatus
import io.airbyte.test.AtcConfig
import io.airbyte.test.AtcData
import io.airbyte.test.AtcDataProperty
import io.github.oshai.kotlinlogging.KotlinLogging
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Tags
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import kotlin.time.Duration.Companion.minutes

private val log = KotlinLogging.logger { }

/**
 * Connector Builder-only acceptance tests.
 * Uses httpbin.org (Postman's public HTTP testing service) as the test data source.
 * This allows the test to work with both local and remote Airbyte deployments.
 * todo(cgardens) - I would hope consolidating builder endpoints into the server would remove the need for this to be tested at the acceptance test level.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Tags(Tag("builder"), Tag("enterprise"))
class ConnectorBuilderTest {
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
  fun `connector builder publish`() {
    val connectorBuilderProject = atClient.admin.createConnectorBuilderProject()
    val srcDefId =
      atClient.admin.publishConnectorBuilderProject(
        projectId = connectorBuilderProject,
        manifest = manifest(getEchoServerUrl()),
        spec = spec,
      )

    val srcId =
      atClient.admin.createSource(
        srcDefId = srcDefId,
        cfg = """{"__injected_declarative_manifest": {}}""",
      )
    val srcCatalog = atClient.admin.discoverSource(srcId).catalog ?: throw RuntimeException("failed to retrieve source catalog")

    // the source will return three records; id:1, id:2, and id:3 configure the destination to expect these three records
    data class Record(
      val id: Int,
    )
    val dstId =
      atClient.admin.createAtcDestination(
        AtcConfig(
          data =
            object : AtcData {
              override fun cursor() = listOf("id")

              override fun required() = listOf("id")

              override fun properties() = mapOf("id" to AtcDataProperty(type = "number"))

              override fun records() = listOf(Record(1), Record(2), Record(3))
            },
        ),
      )

    val conId =
      atClient.admin.createConnection(
        ConnectionCreate(
          sourceId = srcId,
          destinationId = dstId,
          status = ConnectionStatus.ACTIVE,
          syncCatalog = srcCatalog,
        ),
      )

    val jobId = atClient.admin.syncConnection(conId)
    val status = atClient.admin.jobWatchUntilTerminal(jobId, duration = 5.minutes)
    if (status != JobStatus.SUCCEEDED) {
      atClient.admin.jobLogs(jobId, log)
    }

    assertEquals(JobStatus.SUCCEEDED, status)
  }

  /**
   * Returns the base URL for the HTTP test endpoint.
   *
   * Uses httpbin.org (Postman's public HTTP testing service) to enable testing
   * against both local and remote Airbyte deployments.
   *
   * SECURITY NOTE: This sends data to an external service. Only harmless test data
   * is sent (see manifest() function for payload details). DO NOT modify this test
   * to send any sensitive information, credentials, or real data.
   *
   * Data sent: {"records":[{"id":1},{"id":2},{"id":3}]}
   */
  private fun getEchoServerUrl(): String = System.getenv("ACCEPTANCE_ECHO_SERVER_URL") ?: "https://httpbin.org"
}

private val spec =
  $$"""
  {
    "connectionSpecification": {
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "required": [],
      "properties": {},
      "additionalProperties": true
    },
    "documentationUrl": "https://example.org",
    "type": "Spec"
  }
  """.trimIndent()

private fun manifest(urlBase: String): String =
  $$"""
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
                "$schema": "http://json-schema.org/schema#",
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
              "url_base": "$$urlBase",
              "path": "/post",
              "http_method": "POST",
              "request_parameters": {},
              "request_headers": {},
              "request_body_json": "{\"records\":[{\"id\":1},{\"id\":2},{\"id\":3}]}",
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
          "$schema": "http://json-schema.org/draft-07/schema#",
          "type": "object",
          "required": [],
          "properties": {},
          "additionalProperties": true
        },
        "documentation_url": "https://example.org",
        "type": "Spec"
      }
    }
  """.trimIndent()
