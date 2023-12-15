/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.acceptance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.WebBackendApi;
import io.airbyte.api.client.invoker.generated.ApiClient;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.AirbyteCatalog;
import io.airbyte.api.client.model.generated.AirbyteStream;
import io.airbyte.api.client.model.generated.AirbyteStreamAndConfiguration;
import io.airbyte.api.client.model.generated.AirbyteStreamConfiguration;
import io.airbyte.api.client.model.generated.ConnectionRead;
import io.airbyte.api.client.model.generated.ConnectionStatus;
import io.airbyte.api.client.model.generated.ConnectionUpdate;
import io.airbyte.api.client.model.generated.DestinationSyncMode;
import io.airbyte.api.client.model.generated.JobRead;
import io.airbyte.api.client.model.generated.NonBreakingChangesPreference;
import io.airbyte.api.client.model.generated.SchemaChange;
import io.airbyte.api.client.model.generated.SourceDiscoverSchemaRead;
import io.airbyte.api.client.model.generated.SyncMode;
import io.airbyte.api.client.model.generated.WebBackendConnectionRead;
import io.airbyte.api.client.model.generated.WebBackendConnectionRequestBody;
import io.airbyte.api.client.model.generated.WorkspaceCreate;
import io.airbyte.commons.json.Jsons;
import io.airbyte.test.utils.AcceptanceTestHarness;
import io.airbyte.test.utils.Asserts;
import io.airbyte.test.utils.TestConnectionCreate;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for the various schema management functionalities e.g., auto-detect, auto-propagate.
 */
@DisabledIfEnvironmentVariable(named = "SKIP_BASIC_ACCEPTANCE_TESTS",
                               matches = "true")
@Timeout(value = 2,
         unit = TimeUnit.MINUTES) // Default timeout of 2 minutes; individual tests should override if they need longer.
@TestInstance(Lifecycle.PER_CLASS)
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
  public static final int JITTER_MAX_INTERVAL_SECS = 10;
  public static final int FINAL_INTERVAL_SECS = 60;
  public static final int MAX_TRIES = 3;
  public static final String A_NEW_COLUMN = "a_new_column";
  public static final String FIELD_NAME = "name";
  private static final int DEFAULT_VALUE = 50;
  private static AcceptanceTestHarness testHarness;
  private static AirbyteApiClient apiClient;
  private static WebBackendApi webBackendApi;
  private static ConnectionRead createdConnection;
  private static ConnectionRead createdConnectionWithSameSource;

  private void createTestConnections() throws Exception {
    final UUID sourceId = testHarness.createPostgresSource().getSourceId();
    final SourceDiscoverSchemaRead discoverResult = testHarness.discoverSourceSchemaWithId(sourceId);
    final AirbyteCatalog catalog = discoverResult.getCatalog();
    final UUID destinationId = testHarness.createPostgresDestination().getDestinationId();
    final UUID normalizationOpId = testHarness.createNormalizationOperation().getOperationId();
    // Use incremental append-dedup with a primary key column, so we can simulate a breaking change by
    // removing that column.
    final SyncMode syncMode = SyncMode.INCREMENTAL;
    final DestinationSyncMode destinationSyncMode = DestinationSyncMode.APPEND_DEDUP;
    catalog.getStreams().forEach(s -> s.getConfig().syncMode(syncMode).destinationSyncMode(destinationSyncMode).fieldSelectionEnabled(false)
        .primaryKey(List.of(List.of("id")))
        .cursorField(List.of("id")));
    createdConnection =
        testHarness.createConnection(new TestConnectionCreate.Builder(
            sourceId,
            destinationId,
            catalog,
            discoverResult.getCatalogId())
                .setNormalizationOperationId(normalizationOpId)
                .build());
    LOGGER.info("Created connection: {}", createdConnection);
    // Create a connection that shares the source, to verify that the schema management actions are
    // applied to all connections with the same source.
    createdConnectionWithSameSource = testHarness.createConnection(new TestConnectionCreate.Builder(
        createdConnection.getSourceId(),
        createdConnection.getDestinationId(),
        createdConnection.getSyncCatalog(),
        createdConnection.getSourceCatalogId())
            .setAdditionalOperationIds(createdConnection.getOperationIds())
            .setSchedule(createdConnection.getScheduleType(), createdConnection.getScheduleData())
            .setNameSuffix("-same-source")
            .build());
  }

  @BeforeAll
  static void init() throws ApiException, URISyntaxException, IOException, InterruptedException {
    // TODO(mfsiega-airbyte): clean up and centralize the way we do config.
    final boolean isGke = System.getenv().containsKey(IS_GKE);
    // Set up the API client.
    final URI url = new URI(AIRBYTE_SERVER_HOST);
    final var underlyingApiClient = new ApiClient().setScheme(url.getScheme())
        .setHost(url.getHost())
        .setPort(url.getPort())
        .setBasePath("/api");
    if (isGke) {
      underlyingApiClient.setRequestInterceptor(builder -> builder.setHeader(GATEWAY_AUTH_HEADER, AIRBYTE_AUTH_HEADER));
    }
    apiClient = new AirbyteApiClient(underlyingApiClient);

    // Set up the WebBackend API client.
    final var underlyingWebBackendApiClient = new ApiClient().setScheme(url.getScheme())
        .setHost(url.getHost())
        .setPort(url.getPort())
        .setBasePath("/api");
    if (isGke) {
      underlyingWebBackendApiClient.setRequestInterceptor(builder -> builder.setHeader(GATEWAY_AUTH_HEADER, AIRBYTE_AUTH_HEADER));
    }
    webBackendApi = new WebBackendApi(underlyingWebBackendApiClient);

    final UUID workspaceId = System.getenv().get(AIRBYTE_ACCEPTANCE_TEST_WORKSPACE_ID) == null ? apiClient.getWorkspaceApi()
        .createWorkspace(new WorkspaceCreate().email("acceptance-tests@airbyte.io").name("Airbyte Acceptance Tests" + UUID.randomUUID()))
        .getWorkspaceId()
        : UUID.fromString(System.getenv().get(AIRBYTE_ACCEPTANCE_TEST_WORKSPACE_ID));

    testHarness = new AcceptanceTestHarness(apiClient, workspaceId);
  }

  @BeforeEach
  void beforeEach() throws Exception {
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
   * refreshSchema=true, then: - We'll detect schema changes for the given connection. - We'll detect
   * schema changes for any connections sharing the same source.
   */
  @Test
  void detectBreakingSchemaChangeViaWebBackendGetConnection() throws Exception {
    // Modify the underlying source to remove the id column, which is the primary key.
    testHarness.runSqlScriptInSource("postgres_remove_id_column.sql");

    final WebBackendConnectionRead getConnectionAndRefresh = AirbyteApiClient.retryWithJitterThrows(() -> webBackendApi.webBackendGetConnection(
        new WebBackendConnectionRequestBody().connectionId(createdConnection.getConnectionId()).withRefreshedCatalog(true)),
        "get connection and refresh schema", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
    assertEquals(SchemaChange.BREAKING, getConnectionAndRefresh.getSchemaChange());

    final var currentConnection = testHarness.getConnection(createdConnection.getConnectionId());
    assertEquals(createdConnection.getSyncCatalog(), currentConnection.getSyncCatalog());
    assertEquals(ConnectionStatus.INACTIVE, currentConnection.getStatus());

    final ConnectionRead currentConnectionWithSameSource = testHarness.getConnection(createdConnectionWithSameSource.getConnectionId());
    assertTrue(currentConnectionWithSameSource.getBreakingChange());
    assertEquals(createdConnectionWithSameSource.getSyncCatalog(), currentConnectionWithSameSource.getSyncCatalog());
    assertEquals(ConnectionStatus.INACTIVE, currentConnectionWithSameSource.getStatus());
  }

  @Test
  @Timeout(
           value = 10,
           unit = TimeUnit.MINUTES)
  void testPropagateAllChangesViaSyncRefresh() throws Exception {
    // Update one connection to apply all (column + stream) changes.
    AirbyteApiClient.retryWithJitter(() -> apiClient.getConnectionApi().updateConnection(
        new ConnectionUpdate()
            .connectionId(createdConnection.getConnectionId())
            .nonBreakingChangesPreference(NonBreakingChangesPreference.PROPAGATE_FULLY)),
        "update connection non breaking change preference", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);

    // Modify the underlying source to add a new column and a new table, and populate them with some
    // data.
    testHarness.runSqlScriptInSource("postgres_add_column_and_table.sql");
    // Sync the connection, which will trigger a refresh. Wait for it to finish, because we don't have a
    // better way to know when the catalog
    // refresh step is complete.
    final var jobRead = testHarness.syncConnection(createdConnection.getConnectionId()).getJob();
    testHarness.waitForSuccessfulSyncNoTimeout(jobRead);

    // This connection has auto propagation enabled, so we expect it to be updated.
    final var currentConnection = testHarness.getConnection(createdConnection.getConnectionId());
    final AirbyteCatalog catalogWithPropagatedChanges = getExpectedCatalogWithExtraColumnAndTable();
    assertEquals(catalogWithPropagatedChanges, currentConnection.getSyncCatalog());
    assertEquals(ConnectionStatus.ACTIVE, currentConnection.getStatus());
    Asserts.assertNormalizedDestinationContains(testHarness.getDestinationDatabase(), currentConnection.getNamespaceFormat(),
        getExpectedRecordsForIdAndNameWithUpdatedCatalog());

    // This connection does not have auto propagation, so it should have stayed the same.
    final ConnectionRead currentConnectionWithSameSource = testHarness.getConnection(createdConnectionWithSameSource.getConnectionId());
    assertFalse(currentConnectionWithSameSource.getBreakingChange());
    assertEquals(createdConnectionWithSameSource.getSyncCatalog(), currentConnectionWithSameSource.getSyncCatalog());
  }

  @Test
  @Timeout(
           value = 10,
           unit = TimeUnit.MINUTES)
  void testBackfillOnNewColumn() throws Exception {
    AirbyteApiClient.retryWithJitter(() -> apiClient.getConnectionApi().updateConnection(
        new ConnectionUpdate()
            .connectionId(createdConnection.getConnectionId())
            .nonBreakingChangesPreference(NonBreakingChangesPreference.PROPAGATE_FULLY)),
        "update connection non breaking change preference", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS, MAX_TRIES);
    // We sometimes get a race where the connection manager isn't ready, and this results in
    // `syncConnection` simply
    // hanging. This is a bug we should fix in the backend, but we tolerate it here for now.
    Thread.sleep(1000 * 5);
    // Run a sync with the initial data.
    final var jobRead = testHarness.syncConnection(createdConnection.getConnectionId()).getJob();
    testHarness.waitForSuccessfulSyncNoTimeout(jobRead);
    Asserts.assertNormalizedDestinationContains(testHarness.getDestinationDatabase(), createdConnection.getNamespaceFormat(),
        getExpectedRecordsForIdAndName());

    // Modify the source to add a new column, which will be populated with a default value.
    testHarness.runSqlScriptInSource("postgres_add_column_with_default_value.sql");

    // Sync again. This should update the schema, and also run a backfill for the affected stream.
    final JobRead jobReadWithBackfills = testHarness.syncConnection(createdConnection.getConnectionId()).getJob();
    testHarness.waitForSuccessfulSyncNoTimeout(jobReadWithBackfills);
    final var currentConnection = testHarness.getConnection(createdConnection.getConnectionId());
    // Expect that we have the two original fields, plus the new one.
    assertEquals(3, currentConnection.getSyncCatalog().getStreams().get(0).getStream().getJsonSchema().get("properties").size());
    Asserts.assertNormalizedDestinationContains(testHarness.getDestinationDatabase(), createdConnection.getNamespaceFormat(),
        getExpectedRecordsForIdAndNameWithBackfilledColumn());
  }

  private List<JsonNode> getExpectedRecordsForIdAndName() {
    final var nodeFactory = JsonNodeFactory.withExactBigDecimals(false);
    return List.of(
        new ObjectNode(nodeFactory).put("id", 1).put(FIELD_NAME, "sherif"),
        new ObjectNode(nodeFactory).put("id", 2).put(FIELD_NAME, "charles"),
        new ObjectNode(nodeFactory).put("id", 3).put(FIELD_NAME, "jared"),
        new ObjectNode(nodeFactory).put("id", 4).put(FIELD_NAME, "michel"),
        new ObjectNode(nodeFactory).put("id", 5).put(FIELD_NAME, "john"));
  }

  private List<JsonNode> getExpectedRecordsForIdAndNameWithUpdatedCatalog() {
    final var nodeFactory = JsonNodeFactory.withExactBigDecimals(false);
    return List.of(
        new ObjectNode(nodeFactory).put("id", 1).put(FIELD_NAME, "sherif").put(A_NEW_COLUMN, (String) null),
        new ObjectNode(nodeFactory).put("id", 2).put(FIELD_NAME, "charles").put(A_NEW_COLUMN, (String) null),
        new ObjectNode(nodeFactory).put("id", 3).put(FIELD_NAME, "jared").put(A_NEW_COLUMN, (String) null),
        new ObjectNode(nodeFactory).put("id", 4).put(FIELD_NAME, "michel").put(A_NEW_COLUMN, (String) null),
        new ObjectNode(nodeFactory).put("id", 5).put(FIELD_NAME, "john").put(A_NEW_COLUMN, (String) null),
        new ObjectNode(nodeFactory).put("id", 6).put(FIELD_NAME, "a-new-name").put(A_NEW_COLUMN, "contents-of-the-new-column"));
  }

  private List<JsonNode> getExpectedRecordsForIdAndNameWithBackfilledColumn() {
    final var nodeFactory = JsonNodeFactory.withExactBigDecimals(false);
    return List.of(
        new ObjectNode(nodeFactory).put("id", 1).put(FIELD_NAME, "sherif").put(A_NEW_COLUMN, DEFAULT_VALUE),
        new ObjectNode(nodeFactory).put("id", 2).put(FIELD_NAME, "charles").put(A_NEW_COLUMN, DEFAULT_VALUE),
        new ObjectNode(nodeFactory).put("id", 3).put(FIELD_NAME, "jared").put(A_NEW_COLUMN, DEFAULT_VALUE),
        new ObjectNode(nodeFactory).put("id", 4).put(FIELD_NAME, "michel").put(A_NEW_COLUMN, DEFAULT_VALUE),
        new ObjectNode(nodeFactory).put("id", 5).put(FIELD_NAME, "john").put(A_NEW_COLUMN, DEFAULT_VALUE),
        // Note: this is a new record, with a different value from the default.
        new ObjectNode(nodeFactory).put("id", 6).put(FIELD_NAME, "a-new-name").put(A_NEW_COLUMN, 100));
  }

  private AirbyteCatalog getExpectedCatalogWithExtraColumnAndTable() {
    // We have an extra column and an extra stream.
    final var expectedCatalog = Jsons.clone(createdConnection.getSyncCatalog());
    expectedCatalog.getStreams().get(0).getStream().jsonSchema(Jsons
        .deserialize("""
                     {
                             "type":"object",
                             "properties":{"id":{"type":"number","airbyte_type":"integer"},"name":{"type":"string"},"a_new_column":{"type":"string"}}
                     }
                     """));
    expectedCatalog.streams(List.of(new AirbyteStreamAndConfiguration()
        .stream(new AirbyteStream()
            .name("a_new_table")
            .namespace("public")
            .supportedSyncModes(List.of(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))
            .defaultCursorField(List.of())
            .sourceDefinedPrimaryKey(List.of())
            .jsonSchema(Jsons.deserialize("""
                                          {
                                                  "type": "object",
                                                  "properties": { "id": { "type": "number", "airbyte_type": "integer" } }
                                           }
                                          """)))
        .config(new AirbyteStreamConfiguration()
            .syncMode(SyncMode.FULL_REFRESH)
            .cursorField(List.of())
            .destinationSyncMode(DestinationSyncMode.OVERWRITE)
            .primaryKey(List.of())
            .aliasName("a_new_table")
            .selected(true)
            .fieldSelectionEnabled(false)),
        expectedCatalog.getStreams().get(0)));
    return expectedCatalog;
  }

}
