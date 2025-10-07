/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
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
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName
import java.net.Inet4Address
import kotlin.time.Duration.Companion.minutes

private val log = KotlinLogging.logger { }

/**
 * Connector Builder-only acceptance tests.
 * todo(cgardens) - I would hope consolidating builder endpoints into the server would remove the need for this to be tested at the acceptance test level.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisabledIfEnvironmentVariable(named = "IS_GKE", matches = "TRUE", disabledReason = "Cloud GKE environment is preventing unsecured http requests")
@Tags(Tag("builder"), Tag("enterprise"))
class ConnectorBuilderTest {
  companion object {
    private lateinit var echoServer: GenericContainer<*>
  }

  private val atClient = AcceptanceTestClient()

  @BeforeAll
  fun setup() {
    echoServer = GenericContainer(DockerImageName.parse(ECHO_SERVER_IMAGE)).withExposedPorts(8080).also { it.start() }
    atClient.setup()
  }

  @AfterAll
  fun tearDownAll() {
    atClient.tearDownAll()
    echoServer.stop()
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

  private fun getEchoServerUrl(): String {
    val host =
      when {
        System.getenv("KUBE_PROCESS_RUNNER_HOST") != null -> System.getenv("KUBE_PROCESS_RUNNER_HOST")
        System.getProperty("os.name")?.startsWith("Mac") == true -> "host.docker.internal"
        else -> Inet4Address.getLocalHost().hostAddress
      }

    return "http://$host:${echoServer.firstMappedPort}"
  }
}

private const val ECHO_SERVER_IMAGE = "mendhak/http-https-echo:37"

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
              "path": "/",
              "http_method": "GET",
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
