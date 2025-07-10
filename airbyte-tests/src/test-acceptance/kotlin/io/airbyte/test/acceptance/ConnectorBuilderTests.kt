/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.acceptance

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.ConnectionCreate
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.api.client.model.generated.ConnectionRead
import io.airbyte.api.client.model.generated.ConnectionStatus
import io.airbyte.api.client.model.generated.ConnectorBuilderProjectDetails
import io.airbyte.api.client.model.generated.ConnectorBuilderProjectWithWorkspaceId
import io.airbyte.api.client.model.generated.ConnectorBuilderPublishRequestBody
import io.airbyte.api.client.model.generated.DeclarativeSourceManifest
import io.airbyte.api.client.model.generated.SourceCreate
import io.airbyte.api.client.model.generated.SourceDefinitionIdRequestBody
import io.airbyte.api.client.model.generated.SourceRead
import io.airbyte.api.client.model.generated.WorkspaceCreate
import io.airbyte.api.client.model.generated.WorkspaceIdRequestBody
import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.test.utils.AcceptanceTestHarness
import io.airbyte.test.utils.AcceptanceTestUtils.createAirbyteAdminApiClient
import io.airbyte.test.utils.AcceptanceTestUtils.modifyCatalog
import io.airbyte.test.utils.Databases.listAllTables
import io.airbyte.test.utils.Databases.retrieveDestinationRecords
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Tags
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestMethodOrder
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName
import java.io.IOException
import java.sql.SQLException
import java.util.Optional
import java.util.UUID

// todo (cgardens) - I would hope consolidating builder endpoints into the server would remove the need for this to be tested at the acceptance test level.

/**
 * Connector Builder-only acceptance tests.
 */
@DisabledIfEnvironmentVariable(named = "IS_GKE", matches = "TRUE", disabledReason = "Cloud GKE environment is preventing unsecured http requests")
@TestMethodOrder(
  MethodOrderer.OrderAnnotation::class,
)
@TestInstance(TestInstance.Lifecycle.PER_CLASS) // The tests methods already have the right visibility, but PMD complains.
// Silence it as it's a bug.
@Tags(Tag("builder"), Tag("enterprise"))
class ConnectorBuilderTests {
  @Test
  @Throws(IOException::class, InterruptedException::class, SQLException::class)
  fun testConnectorBuilderPublish() {
    val sourceDefinitionId = publishSourceDefinitionThroughConnectorBuilder()
    val sourceRead = createSource(sourceDefinitionId)
    try {
      val destinationId = testHarness!!.createPostgresDestination().destinationId
      val connectionRead = createConnection(sourceRead.sourceId, destinationId)
      runConnection(connectionRead.connectionId)

      val destination = testHarness!!.getDestinationDatabase()
      val destinationTables = listAllTables(destination)
      Assertions.assertEquals(1, destinationTables.size)
      Assertions.assertEquals(3, retrieveDestinationRecords(destination, destinationTables.iterator().next().getFullyQualifiedTableName()).size)
    } finally {
      // clean up
      apiClient!!.sourceDefinitionApi.deleteSourceDefinition(SourceDefinitionIdRequestBody(sourceDefinitionId))
    }
  }

  @Throws(IOException::class)
  private fun publishSourceDefinitionThroughConnectorBuilder(): UUID {
    val manifest = aDeclarativeManifest!!.deepCopy<JsonNode>()
    (manifest.at("/streams/0/retriever/requester") as ObjectNode).put("url_base", getEchoServerUrl())

    val connectorBuilderProject =
      apiClient!!
        .connectorBuilderProjectApi
        .createConnectorBuilderProject(
          ConnectorBuilderProjectWithWorkspaceId(
            workspaceId!!,
            ConnectorBuilderProjectDetails("A custom declarative source", null, null, null, null, null, null),
          ),
        )
    return apiClient!!
      .connectorBuilderProjectApi
      .publishConnectorBuilderProject(
        ConnectorBuilderPublishRequestBody(
          workspaceId!!,
          connectorBuilderProject.builderProjectId,
          "A custom declarative source",
          DeclarativeSourceManifest(
            "A description",
            manifest,
            aSpec!!,
            1L,
          ),
          null,
        ),
      ).sourceDefinitionId
  }

  fun getEchoServerUrl(): String =
    String.format(
      "http://%s:%s/",
      testHarness!!.hostname,
      echoServer!!.firstMappedPort,
    )

  companion object {
    private const val ECHO_SERVER_IMAGE = "mendhak/http-https-echo:29"

    private var apiClient: AirbyteApiClient? = null
    private var workspaceId: UUID? = null
    private var testHarness: AcceptanceTestHarness? = null

    private var echoServer: GenericContainer<*>? = null

    private var aDeclarativeManifest: JsonNode? = null
    private var aSpec: JsonNode? = null

    init {
      try {
        aDeclarativeManifest =
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
        aSpec =
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
      } catch (e: JsonProcessingException) {
        throw RuntimeException(e)
      }
    }

    @BeforeAll
    @Throws(Exception::class)
    @JvmStatic
    fun init() {
      apiClient = createAirbyteAdminApiClient()
      workspaceId =
        apiClient!!
          .workspaceApi
          .createWorkspace(
            WorkspaceCreate(
              "Airbyte Acceptance Tests" + UUID.randomUUID(),
              DEFAULT_ORGANIZATION_ID,
              "acceptance-tests@airbyte.io",
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
            ),
          ).workspaceId
      testHarness =
        AcceptanceTestHarness(
          apiClient!!,
          workspaceId!!,
        )
      testHarness!!.setup()

      echoServer = GenericContainer(DockerImageName.parse(ECHO_SERVER_IMAGE)).withExposedPorts(8080)
      echoServer!!.start()
    }

    @AfterAll
    @Throws(IOException::class)
    @JvmStatic
    fun cleanUp() {
      apiClient!!.workspaceApi.deleteWorkspace(
        WorkspaceIdRequestBody(
          workspaceId!!,
          false,
        ),
      )
      echoServer!!.stop()
    }

    @Throws(IOException::class)
    private fun createSource(sourceDefinitionId: UUID): SourceRead {
      val config = ObjectMapper().readTree("{\"__injected_declarative_manifest\": {}\n}")
      return apiClient!!.sourceApi.createSource(
        SourceCreate(
          sourceDefinitionId,
          config,
          workspaceId!!,
          "A custom declarative source",
          null,
          null,
        ),
      )
    }

    @Throws(IOException::class)
    private fun createConnection(
      sourceId: UUID,
      destinationId: UUID,
    ): ConnectionRead {
      val syncCatalog =
        modifyCatalog(
          testHarness!!.discoverSourceSchemaWithoutCache(sourceId),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.of(true),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
        )
      return apiClient!!.connectionApi.createConnection(
        ConnectionCreate(
          sourceId,
          destinationId,
          ConnectionStatus.ACTIVE,
          "name",
          null,
          null,
          null,
          null,
          syncCatalog,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
        ),
      )
    }

    @Throws(IOException::class, InterruptedException::class)
    private fun runConnection(connectionId: UUID) {
      val connectionSyncRead = apiClient!!.connectionApi.syncConnection(ConnectionIdRequestBody(connectionId))
      testHarness!!.waitForSuccessfulJob(connectionSyncRead.job)
    }
  }
}
