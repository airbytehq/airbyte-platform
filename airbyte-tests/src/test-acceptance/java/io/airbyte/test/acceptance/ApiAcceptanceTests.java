/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.acceptance;

import static io.airbyte.test.acceptance.AcceptanceTestsResources.TRUE;
import static io.airbyte.test.utils.AcceptanceTestHarness.COLUMN_ID;
import static io.airbyte.test.utils.AcceptanceTestHarness.COLUMN_NAME;
import static io.airbyte.test.utils.AcceptanceTestUtils.IS_GKE;
import static io.airbyte.test.utils.AcceptanceTestUtils.modifyCatalog;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.api.client.model.generated.AirbyteCatalog;
import io.airbyte.api.client.model.generated.ConnectionStatus;
import io.airbyte.api.client.model.generated.DestinationDefinitionSpecificationRead;
import io.airbyte.api.client.model.generated.DestinationRead;
import io.airbyte.api.client.model.generated.DestinationSyncMode;
import io.airbyte.api.client.model.generated.JobInfoRead;
import io.airbyte.api.client.model.generated.JobStatus;
import io.airbyte.api.client.model.generated.SourceDefinitionSpecificationRead;
import io.airbyte.api.client.model.generated.SourceDiscoverSchemaRead;
import io.airbyte.api.client.model.generated.SourceRead;
import io.airbyte.api.client.model.generated.SyncMode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.db.jdbc.JdbcUtils;
import io.airbyte.test.utils.AcceptanceTestHarness;
import io.airbyte.test.utils.TestConnectionCreate;
import io.micronaut.http.HttpStatus;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.openapitools.client.infrastructure.ClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class tests api functionality.
 * <p>
 * Due to the number of tests here, this set runs only on the docker deployment for speed. The tests
 * here are disabled for Kubernetes as operations take much longer due to Kubernetes pod spin up
 * times and there is little value in re-running these tests since this part of the system does not
 * vary between deployments.
 * <p>
 * We order tests such that earlier tests test more basic behavior relied upon in later tests. e.g.
 * We test that we can create a destination before we test whether we can sync data to it.
 */
@SuppressWarnings({"PMD.JUnitTestsShouldIncludeAssert", "SqlDialectInspection", "SqlNoDataSourceInspection",
  "PMD.AvoidDuplicateLiterals"})
@Tag("api")
class ApiAcceptanceTests {

  private static final Logger LOGGER = LoggerFactory.getLogger(ApiAcceptanceTests.class);

  private static final String DUPLICATE_TEST_IN_GKE =
      "TODO(https://github.com/airbytehq/airbyte-platform-internal/issues/5182): eliminate test duplication";

  private AcceptanceTestsResources testResources;
  private AcceptanceTestHarness testHarness;
  private UUID workspaceId;

  @BeforeEach
  void setup() throws Exception {
    testResources = new AcceptanceTestsResources();
    testResources.init();
    testHarness = testResources.getTestHarness();
    workspaceId = testResources.getWorkspaceId();
    testResources.setup();
  }

  @AfterEach
  void tearDown() {
    testResources.tearDown();
    testResources.end();
  }

  @Test
  void testGetDestinationSpec() throws IOException {
    final UUID destinationDefinitionId = testHarness.getPostgresDestinationDefinitionId();
    final DestinationDefinitionSpecificationRead spec = testHarness.getDestinationDefinitionSpec(destinationDefinitionId,
        workspaceId);
    assertEquals(destinationDefinitionId, spec.getDestinationDefinitionId());
    assertNotNull(spec.getConnectionSpecification());
  }

  @Test
  void testFailedGet404() {
    final var e = assertThrows(ClientException.class, testHarness::getNonExistentResource);
    assertEquals(HttpStatus.NOT_FOUND.getCode(), e.getStatusCode());
  }

  @Test
  @DisabledIfEnvironmentVariable(named = IS_GKE,
                                 matches = TRUE,
                                 disabledReason = DUPLICATE_TEST_IN_GKE)
  void testGetSourceSpec() throws IOException {
    final UUID sourceDefId = testHarness.getPostgresSourceDefinitionId();
    final SourceDefinitionSpecificationRead spec = testHarness.getSourceDefinitionSpec(sourceDefId, workspaceId);
    assertNotNull(spec.getConnectionSpecification());
  }

  @Test
  @DisabledIfEnvironmentVariable(named = IS_GKE,
                                 matches = TRUE,
                                 disabledReason = DUPLICATE_TEST_IN_GKE)
  void testCreateDestination() throws IOException {
    final UUID destinationDefId = testHarness.getPostgresDestinationDefinitionId();
    final JsonNode destinationConfig = testHarness.getDestinationDbConfig();
    final String name = "AccTestDestinationDb-" + UUID.randomUUID();

    final DestinationRead createdDestination = testHarness.createDestination(
        name,
        workspaceId,
        destinationDefId,
        destinationConfig);
    final var expectedConfig = testHarness.getDestinationDbConfigWithHiddenPassword();
    final var configKeys = List.of("schema", "password", "database", "port", "host", "ssl", "username");

    assertEquals(name, createdDestination.getName());
    assertEquals(destinationDefId, createdDestination.getDestinationDefinitionId());
    assertEquals(workspaceId, createdDestination.getWorkspaceId());
    configKeys.forEach((key) -> {
      if (expectedConfig.get(key).isNumber()) {
        assertEquals(expectedConfig.get(key).asInt(), createdDestination.getConnectionConfiguration().get(key).asInt());
      } else {
        assertEquals(expectedConfig.get(key).asText(), createdDestination.getConnectionConfiguration().get(key).asText());
      }
    });
  }

  @Test
  @DisabledIfEnvironmentVariable(named = IS_GKE,
                                 matches = TRUE,
                                 disabledReason = DUPLICATE_TEST_IN_GKE)
  void testCreateSource() throws IOException {
    final String dbName = "acc-test-db";
    final UUID postgresSourceDefinitionId = testHarness.getPostgresSourceDefinitionId();
    final JsonNode sourceDbConfig = testHarness.getSourceDbConfig();
    final SourceRead response = testHarness.createSource(
        dbName,
        workspaceId,
        postgresSourceDefinitionId,
        sourceDbConfig);

    final var expectedConfig = Jsons.jsonNode(sourceDbConfig);
    final var configKeys = List.of("password", "database", "port", "host", "ssl", "username");

    // expect replacement of secret with magic string.
    ((ObjectNode) expectedConfig).put(JdbcUtils.PASSWORD_KEY, "**********");
    assertEquals(dbName, response.getName());
    assertEquals(workspaceId, response.getWorkspaceId());
    assertEquals(postgresSourceDefinitionId, response.getSourceDefinitionId());
    configKeys.forEach((key) -> {
      if (expectedConfig.get(key).isNumber()) {
        assertEquals(expectedConfig.get(key).asInt(), response.getConnectionConfiguration().get(key).asInt());
      } else {
        assertEquals(expectedConfig.get(key).asText(), response.getConnectionConfiguration().get(key).asText());
      }
    });
  }

  @Test
  @DisabledIfEnvironmentVariable(named = IS_GKE,
                                 matches = TRUE,
                                 disabledReason = DUPLICATE_TEST_IN_GKE)
  void testDiscoverSourceSchema() throws IOException {
    final UUID sourceId = testHarness.createPostgresSource().getSourceId();

    final AirbyteCatalog actual = testHarness.discoverSourceSchema(sourceId);

    testHarness.compareCatalog(actual);
  }

  @Test
  void testDeleteConnection() throws Exception {
    final UUID sourceId = testHarness.createPostgresSource().getSourceId();
    final UUID destinationId = testHarness.createPostgresDestination().getDestinationId();
    final SourceDiscoverSchemaRead discoverResult = testHarness.discoverSourceSchemaWithId(sourceId);
    final SyncMode srcSyncMode = SyncMode.INCREMENTAL;
    final DestinationSyncMode dstSyncMode = DestinationSyncMode.APPEND_DEDUP;
    final AirbyteCatalog catalog = modifyCatalog(
        discoverResult.getCatalog(),
        Optional.of(srcSyncMode),
        Optional.of(dstSyncMode),
        Optional.of(List.of(COLUMN_ID)),
        Optional.of(List.of(List.of(COLUMN_NAME))),
        Optional.of(true),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());

    final UUID connectionId =
        testHarness.createConnection(new TestConnectionCreate.Builder(
            sourceId,
            destinationId,
            catalog,
            discoverResult.getCatalogId())
                .build())
            .getConnectionId();

    final JobInfoRead connectionSyncRead = testHarness.syncConnection(connectionId);
    testHarness.waitWhileJobHasStatus(connectionSyncRead.getJob(), Set.of(JobStatus.RUNNING));

    // test normal deletion of connection
    LOGGER.info("Calling delete connection...");
    testHarness.deleteConnection(connectionId);
    testHarness.removeConnection(connectionId); // NOTE: make sure we don't try to delete it again in test teardown.

    ConnectionStatus connectionStatus = testHarness.getConnection(connectionId).getStatus();
    assertEquals(ConnectionStatus.DEPRECATED, connectionStatus);

    // test that repeated deletion call for same connection is successful
    LOGGER.info("Calling delete connection a second time to test repeat call behavior...");
    assertDoesNotThrow(() -> testHarness.deleteConnection(connectionId));

    // TODO: break this into a separate testcase which we can disable for GKE.
    if (!System.getenv().containsKey("IS_GKE")) {
      // test deletion of connection when temporal workflow is in a bad state
      LOGGER.info("Testing connection deletion when temporal is in a terminal state");
      final var anotherConnectionId =
          testHarness.createConnection(new TestConnectionCreate.Builder(
              sourceId,
              destinationId,
              catalog,
              discoverResult.getCatalogId())
                  .build())
              .getConnectionId();

      testHarness.terminateTemporalWorkflow(anotherConnectionId);

      // we should still be able to delete the connection when the temporal workflow is in this state
      testHarness.deleteConnection(anotherConnectionId);

      connectionStatus = testHarness.getConnection(anotherConnectionId).getStatus();
      assertEquals(ConnectionStatus.DEPRECATED, connectionStatus);
    }
  }

}
