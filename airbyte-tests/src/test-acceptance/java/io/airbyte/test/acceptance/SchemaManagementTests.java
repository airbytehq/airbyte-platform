/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.acceptance;

import static io.airbyte.config.persistence.OrganizationPersistence.DEFAULT_ORGANIZATION_ID;
import static io.airbyte.test.utils.AcceptanceTestUtils.createAirbyteApiClient;
import static io.airbyte.test.utils.AcceptanceTestUtils.modifyCatalog;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import io.airbyte.api.client.model.generated.AirbyteCatalog;
import io.airbyte.api.client.model.generated.AirbyteStream;
import io.airbyte.api.client.model.generated.AirbyteStreamAndConfiguration;
import io.airbyte.api.client.model.generated.AirbyteStreamConfiguration;
import io.airbyte.api.client.model.generated.ConnectionRead;
import io.airbyte.api.client.model.generated.ConnectionStatus;
import io.airbyte.api.client.model.generated.DestinationSyncMode;
import io.airbyte.api.client.model.generated.JobRead;
import io.airbyte.api.client.model.generated.NonBreakingChangesPreference;
import io.airbyte.api.client.model.generated.SchemaChange;
import io.airbyte.api.client.model.generated.SchemaChangeBackfillPreference;
import io.airbyte.api.client.model.generated.SourceDiscoverSchemaRead;
import io.airbyte.api.client.model.generated.SyncMode;
import io.airbyte.api.client.model.generated.WebBackendConnectionRead;
import io.airbyte.api.client.model.generated.WorkspaceCreate;
import io.airbyte.commons.json.Jsons;
import io.airbyte.test.utils.AcceptanceTestHarness;
import io.airbyte.test.utils.TestConnectionCreate;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for the various schema management functionalities e.g., auto-detect, auto-propagate.
 *
 * These tests need the `refreshSchema.period.hours` feature flag to return `0`, otherwise asserts
 * will fail.
 */
@Timeout(value = 2,
         unit = TimeUnit.MINUTES) // Default timeout of 2 minutes; individual tests should override if they need longer.
@Execution(ExecutionMode.CONCURRENT)
@Tag("api")
class SchemaManagementTests {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaManagementTests.class);

  // NOTE: this is all copied from BasicAcceptanceTests. We should refactor to have this in a single
  // place.
  private static final String IS_GKE = "IS_GKE";
  private static final String GATEWAY_AUTH_HEADER = "X-Endpoint-API-UserInfo";
  // NOTE: this is just a base64 encoding of a jwt representing a test user in some deployments.
  private static final String AIRBYTE_AUTH_HEADER = "eyJ1c2VyX2lkIjogImNsb3VkLWFwaSIsICJlbWFpbF92ZXJpZmllZCI6ICJ0cnVlIn0K";
  private static final String AIRBYTE_ACCEPTANCE_TEST_WORKSPACE_ID = "AIRBYTE_ACCEPTANCE_TEST_WORKSPACE_ID";
  private static final String AIRBYTE_SERVER_HOST = Optional.ofNullable(System.getenv("AIRBYTE_SERVER_HOST")).orElse("http://localhost:8001");
  public static final String A_NEW_COLUMN = "a_new_column";
  public static final String FIELD_NAME = "name";
  private static final int DEFAULT_VALUE = 50;
  private AcceptanceTestHarness testHarness;
  private ConnectionRead createdConnection;
  private ConnectionRead createdConnectionWithSameSource;

  private void createTestConnections() throws Exception {
    final UUID sourceId = testHarness.createPostgresSource().getSourceId();
    final SourceDiscoverSchemaRead discoverResult = testHarness.discoverSourceSchemaWithId(sourceId);
    final UUID destinationId = testHarness.createPostgresDestination().getDestinationId();
    // Use incremental append-dedup with a primary key column, so we can simulate a breaking change by
    // removing that column.
    final SyncMode syncMode = SyncMode.INCREMENTAL;
    final DestinationSyncMode destinationSyncMode = DestinationSyncMode.APPEND_DEDUP;
    final AirbyteCatalog catalog = modifyCatalog(
        discoverResult.getCatalog(),
        Optional.of(syncMode),
        Optional.of(destinationSyncMode),
        Optional.of(List.of("id")),
        Optional.of(List.of(List.of("id"))),
        Optional.empty(),
        Optional.of(false),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
    createdConnection =
        testHarness.createConnection(new TestConnectionCreate.Builder(
            sourceId,
            destinationId,
            catalog,
            discoverResult.getCatalogId(),
            testHarness.getDataplaneGroupId()).build());
    LOGGER.info("Created connection: {}", createdConnection);
    // Create a connection that shares the source, to verify that the schema management actions are
    // applied to all connections with the same source.
    createdConnectionWithSameSource = testHarness.createConnection(new TestConnectionCreate.Builder(
        createdConnection.getSourceId(),
        createdConnection.getDestinationId(),
        createdConnection.getSyncCatalog(),
        createdConnection.getSourceCatalogId(),
        createdConnection.getDataplaneGroupId())
            .setAdditionalOperationIds(createdConnection.getOperationIds())
            .setSchedule(createdConnection.getScheduleType(), createdConnection.getScheduleData())
            .setNameSuffix("-same-source")
            .build());
  }

  private void init() throws URISyntaxException, IOException, InterruptedException, GeneralSecurityException {
    // Set up the API client.
    final var airbyteApiClient = createAirbyteApiClient(AIRBYTE_SERVER_HOST + "/api", Map.of(GATEWAY_AUTH_HEADER, AIRBYTE_AUTH_HEADER));

    final UUID workspaceId = System.getenv().get(AIRBYTE_ACCEPTANCE_TEST_WORKSPACE_ID) == null ? airbyteApiClient.getWorkspaceApi()
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
        .getWorkspaceId()
        : UUID.fromString(System.getenv().get(AIRBYTE_ACCEPTANCE_TEST_WORKSPACE_ID));

    testHarness = new AcceptanceTestHarness(airbyteApiClient, workspaceId);
  }

  @BeforeEach
  void beforeEach() throws Exception {
    init();
    LOGGER.debug("Executing test case setup");
    testHarness.setup();
    createTestConnections();
  }

  @AfterEach
  void afterEach() {
    LOGGER.debug("Executing test case teardown");
    testHarness.cleanup();
  }

  /**
   * Verify that if we call web_backend/connections/get with some connection id and
   * refreshSchema=true, then: - We'll detect schema changes for the given connection. - We do not
   * evaluate schema changes for other connections.
   */
  @Test
  void detectBreakingSchemaChangeViaWebBackendGetConnection() throws Exception {
    // Modify the underlying source to remove the id column, which is the primary key.
    testHarness.runSqlScriptInSource("postgres_remove_id_column.sql");
    final WebBackendConnectionRead getConnectionAndRefresh = testHarness.webBackendGetConnectionAndRefreshSchema(createdConnection.getConnectionId());
    assertEquals(SchemaChange.BREAKING, getConnectionAndRefresh.getSchemaChange());

    final var currentConnection = testHarness.getConnection(createdConnection.getConnectionId());
    assertEquals(createdConnection.getSyncCatalog(), currentConnection.getSyncCatalog());
    assertEquals(ConnectionStatus.INACTIVE, currentConnection.getStatus());

    final ConnectionRead currentConnectionWithSameSource = testHarness.getConnection(createdConnectionWithSameSource.getConnectionId());
    assertFalse(currentConnectionWithSameSource.getBreakingChange());
    assertEquals(ConnectionStatus.ACTIVE, currentConnectionWithSameSource.getStatus());
  }

  @Test
  @Timeout(
           value = 10,
           unit = TimeUnit.MINUTES)
  void testPropagateAllChangesViaSyncRefresh() throws Exception {
    // Update one connection to apply all (column + stream) changes.
    testHarness.updateSchemaChangePreference(createdConnection.getConnectionId(), NonBreakingChangesPreference.PROPAGATE_FULLY, null);

    // Modify the underlying source to add a new column and a new table, and populate them with some
    // data.
    testHarness.runSqlScriptInSource("postgres_add_column_and_table.sql");
    // Sync the connection, which will trigger a refresh. Wait for it to finish, because we don't have a
    // better way to know when the catalog refresh step is complete.
    final var jobRead = testHarness.syncConnection(createdConnection.getConnectionId()).getJob();
    testHarness.waitForSuccessfulSyncNoTimeout(jobRead);

    // This connection has auto propagation enabled, so we expect it to be updated.
    final var currentConnection = testHarness.getConnection(createdConnection.getConnectionId());
    final AirbyteCatalog catalogWithPropagatedChanges = getExpectedCatalogWithExtraColumnAndTable();
    assertEquals(catalogWithPropagatedChanges, currentConnection.getSyncCatalog());
    assertEquals(ConnectionStatus.ACTIVE, currentConnection.getStatus());

    // This connection does not have auto propagation, so it should have stayed the same.
    final ConnectionRead currentConnectionWithSameSource = testHarness.getConnection(createdConnectionWithSameSource.getConnectionId());
    assertFalse(currentConnectionWithSameSource.getBreakingChange());
    assertEquals(createdConnectionWithSameSource.getSyncCatalog(), currentConnectionWithSameSource.getSyncCatalog());
  }

  @Test
  @Timeout(
           value = 10,
           unit = TimeUnit.MINUTES)
  void testBackfillDisabled() throws Exception {
    testHarness.updateSchemaChangePreference(createdConnection.getConnectionId(), NonBreakingChangesPreference.PROPAGATE_FULLY,
        SchemaChangeBackfillPreference.DISABLED);
    // Run a sync with the initial data.
    final var jobRead = testHarness.syncConnection(createdConnection.getConnectionId()).getJob();
    testHarness.waitForSuccessfulSyncNoTimeout(jobRead);

    // Modify the source to add a new column and populate it with default values.
    testHarness.runSqlScriptInSource("postgres_add_column_with_default_value.sql");
    testHarness.discoverSourceSchemaWithId(createdConnection.getSourceId());

    // Sync again. This should update the schema, but it shouldn't backfill, so only the new row should
    // have the new column populated.
    final JobRead jobReadWithBackfills = testHarness.syncConnection(createdConnection.getConnectionId()).getJob();
    testHarness.waitForSuccessfulSyncNoTimeout(jobReadWithBackfills);
    final var currentConnection = testHarness.getConnection(createdConnection.getConnectionId());
    assertEquals(3, currentConnection.getSyncCatalog().getStreams().getFirst().getStream().getJsonSchema().get("properties").size());
  }

  @Test
  @Timeout(
           value = 10,
           unit = TimeUnit.MINUTES)
  void testBackfillOnNewColumn() throws Exception {
    testHarness.updateSchemaChangePreference(createdConnection.getConnectionId(), NonBreakingChangesPreference.PROPAGATE_FULLY,
        SchemaChangeBackfillPreference.ENABLED);
    // Run a sync with the initial data.
    final var jobRead = testHarness.syncConnection(createdConnection.getConnectionId()).getJob();
    testHarness.waitForSuccessfulSyncNoTimeout(jobRead);

    // Modify the source to add a new column, which will be populated with a default value.
    testHarness.runSqlScriptInSource("postgres_add_column_with_default_value.sql");
    testHarness.discoverSourceSchemaWithId(createdConnection.getSourceId());

    // Sync again. This should update the schema, and also run a backfill for the affected stream.
    final JobRead jobReadWithBackfills = testHarness.syncConnection(createdConnection.getConnectionId()).getJob();
    testHarness.waitForSuccessfulSyncNoTimeout(jobReadWithBackfills);
    final var currentConnection = testHarness.getConnection(createdConnection.getConnectionId());
    // Expect that we have the two original fields, plus the new one.
    assertEquals(3, currentConnection.getSyncCatalog().getStreams().getFirst().getStream().getJsonSchema().get("properties").size());
  }

  @Test
  @Timeout(
           value = 10,
           unit = TimeUnit.MINUTES)
  void testApplyEmptySchemaChange() throws Exception {
    // Modify the source to add another stream.
    testHarness.runSqlScriptInSource("postgres_add_column_and_table.sql");
    // Run discover.
    final SourceDiscoverSchemaRead result = testHarness.discoverSourceSchemaWithId(createdConnection.getSourceId());
    // Update the catalog, but don't enable the new stream.
    final ConnectionRead firstUpdate = testHarness.updateConnectionSourceCatalogId(createdConnection.getConnectionId(), result.getCatalogId());
    LOGGER.info("updatedConnection: {}", firstUpdate);
    // Modify the source to add a field to the disabled stream.
    testHarness.runSqlScriptInSource("postgres_add_column_to_new_table.sql");
    // Run a sync.
    final JobRead jobReadWithBackfills = testHarness.syncConnection(createdConnection.getConnectionId()).getJob();
    testHarness.waitForSuccessfulSyncNoTimeout(jobReadWithBackfills);
    // Verify that the catalog is the same, but the source catalog id has been updated.
    final ConnectionRead secondUpdate = testHarness.getConnection(createdConnection.getConnectionId());
    assertEquals(firstUpdate.getSyncCatalog(), secondUpdate.getSyncCatalog());
    assertNotEquals(firstUpdate.getSourceCatalogId(), secondUpdate.getSourceCatalogId());
  }

  private AirbyteCatalog getExpectedCatalogWithExtraColumnAndTable() {
    final var existingStreamAndConfig = createdConnection.getSyncCatalog().getStreams().getFirst();

    final var streams = new ArrayList<AirbyteStreamAndConfiguration>();
    streams.add(new AirbyteStreamAndConfiguration(
        new AirbyteStream(
            existingStreamAndConfig.getStream().getName(),
            Jsons.deserialize("""
                              {
                                      "type":"object",
                                      "properties": {
                                        "id":{"type":"number","airbyte_type":"integer"},
                                        "name":{"type":"string"},
                                        "a_new_column":{"type":"number","airbyte_type":"integer"}
                                      }
                              }
                              """),
            existingStreamAndConfig.getStream().getSupportedSyncModes(),
            existingStreamAndConfig.getStream().getSourceDefinedCursor(),
            existingStreamAndConfig.getStream().getDefaultCursorField(),
            existingStreamAndConfig.getStream().getSourceDefinedPrimaryKey(),
            existingStreamAndConfig.getStream().getNamespace(),
            existingStreamAndConfig.getStream().isResumable(),
            existingStreamAndConfig.getStream().isFileBased()),
        new AirbyteStreamConfiguration(
            existingStreamAndConfig.getConfig().getSyncMode(),
            existingStreamAndConfig.getConfig().getDestinationSyncMode(),
            existingStreamAndConfig.getConfig().getCursorField(),
            null,
            existingStreamAndConfig.getConfig().getPrimaryKey(),
            existingStreamAndConfig.getConfig().getAliasName(),
            existingStreamAndConfig.getConfig().getSelected(),
            existingStreamAndConfig.getConfig().getSuggested(),
            existingStreamAndConfig.getConfig().getFieldSelectionEnabled(),
            existingStreamAndConfig.getConfig().getIncludeFiles(),
            existingStreamAndConfig.getConfig().getSelectedFields(),
            existingStreamAndConfig.getConfig().getHashedFields(),
            existingStreamAndConfig.getConfig().getMappers(),
            existingStreamAndConfig.getConfig().getMinimumGenerationId(),
            existingStreamAndConfig.getConfig().getGenerationId(),
            existingStreamAndConfig.getConfig().getSyncId())));
    streams.add(new AirbyteStreamAndConfiguration(
        new AirbyteStream(
            "a_new_table",
            Jsons.deserialize("""
                              {
                                      "type": "object",
                                      "properties": { "id": { "type": "number", "airbyte_type": "integer" } }
                               }
                              """),
            List.of(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL),
            false,
            List.of(),
            List.of(),
            "public",
            true,
            null),
        new AirbyteStreamConfiguration(
            SyncMode.FULL_REFRESH,
            DestinationSyncMode.OVERWRITE,
            List.of(),
            null,
            List.of(),
            "a_new_table",
            true,
            false,
            false,
            false,
            List.of(),
            List.of(),
            List.of(),
            null,
            null,
            null)));

    streams.sort(Comparator.comparing(a -> a.getStream().getName()));
    return new AirbyteCatalog(streams);
  }

}
