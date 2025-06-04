/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.acceptance;

import static io.airbyte.config.persistence.OrganizationPersistence.DEFAULT_ORGANIZATION_ID;
import static io.airbyte.test.utils.AcceptanceTestUtils.createAirbyteApiClient;
import static io.airbyte.test.utils.AcceptanceTestUtils.modifyCatalog;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.model.generated.AirbyteCatalog;
import io.airbyte.api.client.model.generated.ConnectionCreate;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.ConnectionRead;
import io.airbyte.api.client.model.generated.ConnectionStatus;
import io.airbyte.api.client.model.generated.ConnectorBuilderProjectDetails;
import io.airbyte.api.client.model.generated.ConnectorBuilderProjectIdWithWorkspaceId;
import io.airbyte.api.client.model.generated.ConnectorBuilderProjectWithWorkspaceId;
import io.airbyte.api.client.model.generated.ConnectorBuilderPublishRequestBody;
import io.airbyte.api.client.model.generated.DeclarativeSourceManifest;
import io.airbyte.api.client.model.generated.JobInfoRead;
import io.airbyte.api.client.model.generated.SourceCreate;
import io.airbyte.api.client.model.generated.SourceDefinitionIdRequestBody;
import io.airbyte.api.client.model.generated.SourceRead;
import io.airbyte.api.client.model.generated.WorkspaceCreate;
import io.airbyte.api.client.model.generated.WorkspaceIdRequestBody;
import io.airbyte.db.Database;
import io.airbyte.test.utils.AcceptanceTestHarness;
import io.airbyte.test.utils.Databases;
import io.airbyte.test.utils.SchemaTableNamePair;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Tags;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Connector Builder-only acceptance tests.
 */
@DisabledIfEnvironmentVariable(named = "IS_GKE",
                               matches = "TRUE",
                               disabledReason = "Cloud GKE environment is preventing unsecured http requests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
// The tests methods already have the right visibility, but PMD complains.
// Silence it as it's a bug.
@SuppressWarnings("PMD.JUnit5TestShouldBePackagePrivate")
@Tags({@Tag("builder"), @Tag("enterprise")})
public class ConnectorBuilderTests {

  private static final String ECHO_SERVER_IMAGE = "mendhak/http-https-echo:29";
  private static final String AIRBYTE_SERVER_HOST = Optional.ofNullable(System.getenv("AIRBYTE_SERVER_HOST")).orElse("http://localhost:8001");

  private static AirbyteApiClient apiClient;
  private static UUID workspaceId;
  private static AcceptanceTestHarness testHarness;

  private static GenericContainer echoServer;

  private static final JsonNode A_DECLARATIVE_MANIFEST;
  private static final JsonNode A_SPEC;

  static {
    try {
      A_DECLARATIVE_MANIFEST =
          new ObjectMapper().readTree("""
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
                                                "url_base": "<url_base needs to be update in order to work since port is defined only in @BeforeAll>",
                                                "path": "/",
                                                "http_method": "GET",
                                                "request_parameters": {},
                                                "request_headers": {},
                                                "request_body_json": "{\\"records\\":[{\\"id\\":1},{\\"id\\":2},{\\"id\\":3}]}",
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
                                      }""");
      A_SPEC = new ObjectMapper().readTree("""
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
                                           }""");
    } catch (final JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @BeforeAll
  static void init() throws Exception {
    apiClient = createAirbyteApiClient(AIRBYTE_SERVER_HOST + "/api", Map.of());
    workspaceId = apiClient.getWorkspaceApi()
        .createWorkspace(new WorkspaceCreate(
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
            null))
        .getWorkspaceId();
    testHarness = new AcceptanceTestHarness(apiClient, workspaceId);
    testHarness.setup();

    echoServer = new GenericContainer(DockerImageName.parse(ECHO_SERVER_IMAGE)).withExposedPorts(8080);
    echoServer.start();
  }

  @AfterAll
  static void cleanUp() throws IOException {
    apiClient.getWorkspaceApi().deleteWorkspace(new WorkspaceIdRequestBody(workspaceId, false));
    echoServer.stop();
  }

  @Test
  void testConnectorBuilderPublish() throws IOException, InterruptedException, SQLException {
    final UUID sourceDefinitionId = publishSourceDefinitionThroughConnectorBuilder();
    final SourceRead sourceRead = createSource(sourceDefinitionId);
    try {
      final UUID destinationId = testHarness.createPostgresDestination().getDestinationId();
      final ConnectionRead connectionRead = createConnection(sourceRead.getSourceId(), destinationId);
      runConnection(connectionRead.getConnectionId());

      final Database destination = testHarness.getDestinationDatabase();
      final Set<SchemaTableNamePair> destinationTables = Databases.listAllTables(destination);
      assertEquals(1, destinationTables.size());
      assertEquals(3, Databases.retrieveDestinationRecords(destination, destinationTables.iterator().next().getFullyQualifiedTableName()).size());
    } finally {
      // clean up
      apiClient.getSourceDefinitionApi().deleteSourceDefinition(new SourceDefinitionIdRequestBody(sourceDefinitionId));
    }
  }

  private UUID publishSourceDefinitionThroughConnectorBuilder() throws IOException {
    final JsonNode manifest = A_DECLARATIVE_MANIFEST.deepCopy();
    ((ObjectNode) manifest.at("/streams/0/retriever/requester")).put("url_base", getEchoServerUrl());

    final ConnectorBuilderProjectIdWithWorkspaceId connectorBuilderProject = apiClient.getConnectorBuilderProjectApi()
        .createConnectorBuilderProject(new ConnectorBuilderProjectWithWorkspaceId(
            workspaceId,
            new ConnectorBuilderProjectDetails("A custom declarative source", null, null, null, null, null, null)));
    return apiClient.getConnectorBuilderProjectApi()
        .publishConnectorBuilderProject(new ConnectorBuilderPublishRequestBody(
            workspaceId,
            connectorBuilderProject.getBuilderProjectId(),
            "A custom declarative source",
            new DeclarativeSourceManifest(
                "A description",
                manifest,
                A_SPEC,
                1L),
            null))
        .getSourceDefinitionId();
  }

  private static SourceRead createSource(final UUID sourceDefinitionId) throws IOException {
    final JsonNode config = new ObjectMapper().readTree("{\"__injected_declarative_manifest\": {}\n}");
    return apiClient.getSourceApi().createSource(
        new SourceCreate(
            sourceDefinitionId,
            config,
            workspaceId,
            "A custom declarative source",
            null,
            null));
  }

  private static ConnectionRead createConnection(final UUID sourceId, final UUID destinationId) throws IOException {
    final AirbyteCatalog syncCatalog = modifyCatalog(
        testHarness.discoverSourceSchemaWithoutCache(sourceId),
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
        Optional.empty());
    return apiClient.getConnectionApi().createConnection(
        new ConnectionCreate(
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
            null));
  }

  private static void runConnection(final UUID connectionId) throws IOException, InterruptedException {
    final JobInfoRead connectionSyncRead = apiClient.getConnectionApi().syncConnection(new ConnectionIdRequestBody(connectionId));
    testHarness.waitForSuccessfulJob(connectionSyncRead.getJob());
  }

  private String getEchoServerUrl() {
    return String.format("http://%s:%s/", testHarness.getHostname(), echoServer.getFirstMappedPort());
  }

}
