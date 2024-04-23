/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.acceptance;

import static io.airbyte.test.acceptance.AcceptanceTestsResources.FINAL_INTERVAL_SECS;
import static io.airbyte.test.acceptance.AcceptanceTestsResources.IS_GKE;
import static io.airbyte.test.acceptance.AcceptanceTestsResources.JITTER_MAX_INTERVAL_SECS;
import static io.airbyte.test.acceptance.AcceptanceTestsResources.KUBE;
import static io.airbyte.test.acceptance.AcceptanceTestsResources.MAX_TRIES;
import static io.airbyte.test.acceptance.AcceptanceTestsResources.TRUE;
import static io.airbyte.test.acceptance.AcceptanceTestsResources.WITHOUT_SCD_TABLE;
import static io.airbyte.test.acceptance.AcceptanceTestsResources.WITH_SCD_TABLE;
import static io.airbyte.test.utils.AcceptanceTestHarness.COLUMN_ID;
import static io.airbyte.test.utils.AcceptanceTestHarness.COLUMN_NAME;
import static io.airbyte.test.utils.AcceptanceTestHarness.PUBLIC;
import static io.airbyte.test.utils.AcceptanceTestHarness.PUBLIC_SCHEMA_NAME;
import static io.airbyte.test.utils.AcceptanceTestHarness.STREAM_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.AirbyteCatalog;
import io.airbyte.api.client.model.generated.CheckConnectionRead;
import io.airbyte.api.client.model.generated.ConnectionRead;
import io.airbyte.api.client.model.generated.ConnectionScheduleData;
import io.airbyte.api.client.model.generated.ConnectionScheduleDataCron;
import io.airbyte.api.client.model.generated.ConnectionScheduleType;
import io.airbyte.api.client.model.generated.DestinationSyncMode;
import io.airbyte.api.client.model.generated.JobInfoRead;
import io.airbyte.api.client.model.generated.JobRead;
import io.airbyte.api.client.model.generated.JobStatus;
import io.airbyte.api.client.model.generated.OperationRead;
import io.airbyte.api.client.model.generated.SelectedFieldInfo;
import io.airbyte.api.client.model.generated.SourceDefinitionRead;
import io.airbyte.api.client.model.generated.SourceDiscoverSchemaRead;
import io.airbyte.api.client.model.generated.SourceRead;
import io.airbyte.api.client.model.generated.StreamDescriptor;
import io.airbyte.api.client.model.generated.StreamStatusJobType;
import io.airbyte.api.client.model.generated.StreamStatusRunState;
import io.airbyte.api.client.model.generated.SyncMode;
import io.airbyte.api.client.model.generated.WebBackendConnectionUpdate;
import io.airbyte.commons.json.Jsons;
import io.airbyte.db.Database;
import io.airbyte.test.utils.AcceptanceTestHarness;
import io.airbyte.test.utils.Asserts;
import io.airbyte.test.utils.Databases;
import io.airbyte.test.utils.SchemaTableNamePair;
import io.airbyte.test.utils.TestConnectionCreate;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class tests sync functionality.
 * <p>
 * Due to the number of tests here, this set runs only on the docker deployment for speed. The tests
 * here are disabled for Kubernetes as operations take much longer due to Kubernetes pod spin up
 * times and there is little value in re-running these tests since this part of the system does not
 * vary between deployments.
 * <p>
 * We order tests such that earlier tests test more basic behavior relied upon in later tests. e.g.
 * We test that we can create a destination before we test whether we can sync data to it.
 * <p>
 * Suppressing DataFlowIssue to remove linting of NPEs. It removes a ton of noise and in the case of
 * these tests, the assert statement we would need to put in to check nullability is just as good as
 * throwing the NPE as they will be effectively the same at run time.
 */
@SuppressWarnings({"PMD.JUnitTestsShouldIncludeAssert", "DataFlowIssue", "SqlDialectInspection",
  "SqlNoDataSourceInspection",
  "PMD.AvoidDuplicateLiterals"})
@DisabledIfEnvironmentVariable(named = "SKIP_BASIC_ACCEPTANCE_TESTS",
                               matches = "true")
@Execution(ExecutionMode.CONCURRENT)
class SyncAcceptanceTests {

  private static final Logger LOGGER = LoggerFactory.getLogger(SyncAcceptanceTests.class);

  private AcceptanceTestsResources testResources;

  static final String SLOW_TEST_IN_GKE =
      "TODO(https://github.com/airbytehq/airbyte-platform-internal/issues/5181): re-enable slow tests in GKE";
  static final String DUPLICATE_TEST_IN_GKE =
      "TODO(https://github.com/airbytehq/airbyte-platform-internal/issues/5182): eliminate test duplication";
  static final String TYPE = "type";
  static final String E2E_TEST_SOURCE = "E2E Test Source -";
  static final String INFINITE_FEED = "INFINITE_FEED";
  static final String MESSAGE_INTERVAL = "message_interval";
  static final String MAX_RECORDS = "max_records";
  static final String FIELD = "field";
  static final String ID_AND_NAME = "id_and_name";
  AcceptanceTestHarness testHarness;
  UUID workspaceId;

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
  @DisabledIfEnvironmentVariable(named = IS_GKE,
                                 matches = TRUE,
                                 disabledReason = DUPLICATE_TEST_IN_GKE)
  void testSourceCheckConnection() throws ApiException {
    final UUID sourceId = testHarness.createPostgresSource().getSourceId();

    final CheckConnectionRead checkConnectionRead = testHarness.checkSource(sourceId);

    assertEquals(
        CheckConnectionRead.StatusEnum.SUCCEEDED,
        checkConnectionRead.getStatus(),
        checkConnectionRead.getMessage());
  }

  @Test
  void testCancelSync() throws Exception {
    final SourceDefinitionRead sourceDefinition = testHarness.createE2eSourceDefinition(
        workspaceId);

    final SourceRead source = testHarness.createSource(
        E2E_TEST_SOURCE + UUID.randomUUID(),
        workspaceId,
        sourceDefinition.getSourceDefinitionId(),
        Jsons.jsonNode(ImmutableMap.builder()
            .put(TYPE, INFINITE_FEED)
            .put(MESSAGE_INTERVAL, 1000)
            .put(MAX_RECORDS, Duration.ofMinutes(5).toSeconds())
            .build()));

    final UUID sourceId = source.getSourceId();
    final UUID destinationId = testHarness.createPostgresDestination().getDestinationId();
    final SourceDiscoverSchemaRead discoverResult = testHarness.discoverSourceSchemaWithId(sourceId);
    final AirbyteCatalog catalog = discoverResult.getCatalog();
    final SyncMode srcSyncMode = SyncMode.FULL_REFRESH;
    final DestinationSyncMode dstSyncMode = DestinationSyncMode.OVERWRITE;
    catalog.getStreams().forEach(s -> s.getConfig().syncMode(srcSyncMode).destinationSyncMode(dstSyncMode));
    final UUID connectionId =
        testHarness.createConnection(new TestConnectionCreate.Builder(
            sourceId,
            destinationId,
            catalog,
            discoverResult.getCatalogId())
                .build())
            .getConnectionId();
    final JobInfoRead connectionSyncRead = testHarness.syncConnection(connectionId);

    // wait to get out of PENDING
    final JobRead jobRead = testHarness.waitWhileJobHasStatus(connectionSyncRead.getJob(), Set.of(JobStatus.PENDING));
    assertEquals(JobStatus.RUNNING, jobRead.getStatus());

    final var resp = testHarness.cancelSync(connectionSyncRead.getJob().getId());
    assertEquals(JobStatus.CANCELLED, resp.getJob().getStatus());
  }

  @Test
  @DisabledIfEnvironmentVariable(named = IS_GKE,
                                 matches = TRUE,
                                 disabledReason = DUPLICATE_TEST_IN_GKE)
  void testScheduledSync() throws Exception {
    final UUID sourceId = testHarness.createPostgresSource().getSourceId();
    final UUID destinationId = testHarness.createPostgresDestination().getDestinationId();
    final SourceDiscoverSchemaRead discoverResult = testHarness.discoverSourceSchemaWithId(sourceId);
    final AirbyteCatalog catalog = discoverResult.getCatalog();

    final SyncMode srcSyncMode = SyncMode.FULL_REFRESH;
    final DestinationSyncMode dstSyncMode = DestinationSyncMode.OVERWRITE;
    catalog.getStreams().forEach(s -> s.getConfig().selected(true).syncMode(srcSyncMode).destinationSyncMode(dstSyncMode));

    final var conn = testHarness.createConnection(new TestConnectionCreate.Builder(
        sourceId,
        destinationId,
        catalog,
        discoverResult.getCatalogId())
            .setSchedule(ConnectionScheduleType.BASIC, testResources.getBasicScheduleData())
            .build());
    final var connectionId = conn.getConnectionId();
    final var jobRead = testHarness.getMostRecentSyncForConnection(connectionId);

    testResources.waitForSuccessfulJobWithRetries(jobRead);

    Asserts.assertSourceAndDestinationDbRawRecordsInSync(
        testHarness.getSourceDatabase(), testHarness.getDestinationDatabase(), PUBLIC_SCHEMA_NAME,
        conn.getNamespaceFormat(),
        false, WITHOUT_SCD_TABLE);
    Asserts.assertStreamStatuses(testHarness, workspaceId, connectionId, jobRead.getId(), StreamStatusRunState.COMPLETE, StreamStatusJobType.SYNC);
  }

  @Test
  void testCronSync() throws Exception {
    final UUID sourceId = testHarness.createPostgresSource().getSourceId();
    final UUID destinationId = testHarness.createPostgresDestination().getDestinationId();
    final SourceDiscoverSchemaRead discoverResult = testHarness.discoverSourceSchemaWithId(sourceId);
    final AirbyteCatalog catalog = discoverResult.getCatalog();

    // NOTE: this cron should run once every two minutes.
    final ConnectionScheduleData connectionScheduleData = new ConnectionScheduleData().cron(
        new ConnectionScheduleDataCron().cronExpression("* */2 * * * ?").cronTimeZone("UTC"));
    final SyncMode srcSyncMode = SyncMode.FULL_REFRESH;
    final DestinationSyncMode dstSyncMode = DestinationSyncMode.OVERWRITE;
    catalog.getStreams().forEach(s -> s.getConfig().syncMode(srcSyncMode).selected(true).destinationSyncMode(dstSyncMode));

    final var conn =
        testHarness.createConnection(new TestConnectionCreate.Builder(
            sourceId,
            destinationId,
            catalog,
            discoverResult.getCatalogId()).setSchedule(ConnectionScheduleType.CRON, connectionScheduleData)
                .build());

    final var connectionId = conn.getConnectionId();
    final var jobRead = testHarness.getMostRecentSyncForConnection(connectionId);

    testResources.waitForSuccessfulJobWithRetries(jobRead);

    // NOTE: this is an unusual use of retryWithJitter. Sometimes the raw tables haven't been cleaned up
    // even though the job
    // is marked successful.
    final String retryAssertOutcome = AirbyteApiClient.retryWithJitter(() -> {
      Asserts.assertSourceAndDestinationDbRawRecordsInSync(testHarness.getSourceDatabase(), testHarness.getDestinationDatabase(), PUBLIC_SCHEMA_NAME,
          conn.getNamespaceFormat(),
          false, WITHOUT_SCD_TABLE);
      return "success"; // If the assertion throws after all the retries, then retryWithJitter will return null.
    }, "assert destination in sync", JITTER_MAX_INTERVAL_SECS, FINAL_INTERVAL_SECS,
        MAX_TRIES);
    assertEquals("success", retryAssertOutcome);

    Asserts.assertStreamStatuses(testHarness, workspaceId, connectionId, jobRead.getId(), StreamStatusRunState.COMPLETE, StreamStatusJobType.SYNC);

    testHarness.deleteConnection(connectionId);

    // remove connection to avoid exception during tear down
    testHarness.removeConnection(connectionId);
  }

  @Test
  @DisabledIfEnvironmentVariable(named = IS_GKE,
                                 matches = TRUE,
                                 disabledReason = SLOW_TEST_IN_GKE)
  // NOTE: we also cover incremental dedupe syncs in testIncrementalDedupeSyncRemoveOneColumn below.
  void testIncrementalDedupeSync() throws Exception {
    final UUID sourceId = testHarness.createPostgresSource().getSourceId();
    final UUID destinationId = testHarness.createPostgresDestination().getDestinationId();
    final UUID normalizationOpId = testHarness.createNormalizationOperation().getOperationId();
    final SourceDiscoverSchemaRead discoverResult = testHarness.discoverSourceSchemaWithId(sourceId);
    final AirbyteCatalog catalog = discoverResult.getCatalog();
    final SyncMode srcSyncMode = SyncMode.INCREMENTAL;
    final DestinationSyncMode dstSyncMode = DestinationSyncMode.APPEND_DEDUP;
    catalog.getStreams().forEach(s -> s.getConfig()
        .syncMode(srcSyncMode)
        .selected(true)
        .cursorField(List.of(COLUMN_ID))
        .destinationSyncMode(dstSyncMode)
        .primaryKey(List.of(List.of(COLUMN_NAME))));
    final var conn =
        testHarness.createConnection(new TestConnectionCreate.Builder(
            sourceId,
            destinationId,
            catalog,
            discoverResult.getCatalogId())
                .setNormalizationOperationId(normalizationOpId)
                .build());
    final var connectionId = conn.getConnectionId();
    // sync from start
    final JobInfoRead connectionSyncRead1 = testHarness.syncConnection(connectionId);
    testHarness.waitForSuccessfulJob(connectionSyncRead1.getJob());

    final var dst = testHarness.getDestinationDatabase();
    Asserts.assertSourceAndDestinationDbRawRecordsInSync(testHarness.getSourceDatabase(), dst, PUBLIC_SCHEMA_NAME, conn.getNamespaceFormat(), true,
        WITH_SCD_TABLE);
    Asserts.assertStreamStatuses(testHarness, workspaceId, connectionId, connectionSyncRead1.getJob().getId(), StreamStatusRunState.COMPLETE,
        StreamStatusJobType.SYNC);

    // add new records and run again.
    final Database source = testHarness.getSourceDatabase();
    final List<JsonNode> expectedRawRecords = testHarness.retrieveRecordsFromDatabase(source, STREAM_NAME);
    expectedRawRecords.add(Jsons.jsonNode(ImmutableMap.builder().put(COLUMN_ID, 6).put(COLUMN_NAME, "sherif").build()));
    expectedRawRecords.add(Jsons.jsonNode(ImmutableMap.builder().put(COLUMN_ID, 7).put(COLUMN_NAME, "chris").build()));
    source.query(ctx -> ctx.execute("UPDATE id_and_name SET id=6 WHERE name='sherif'"));
    source.query(ctx -> ctx.execute("INSERT INTO id_and_name(id, name) VALUES(7, 'chris')"));
    // retrieve latest snapshot of source records after modifications; the deduplicated table in
    // destination should mirror this latest state of records
    final List<JsonNode> expectedNormalizedRecords = testHarness.retrieveRecordsFromDatabase(source, STREAM_NAME);

    final JobInfoRead connectionSyncRead2 = testHarness.syncConnection(connectionId);
    testHarness.waitForSuccessfulJob(connectionSyncRead2.getJob());

    Asserts.assertRawDestinationContains(dst, expectedRawRecords, conn.getNamespaceFormat(), STREAM_NAME);
    Asserts.assertNormalizedDestinationContains(dst, conn.getNamespaceFormat(), expectedNormalizedRecords);
    Asserts.assertStreamStatuses(testHarness, workspaceId, connectionId, connectionSyncRead2.getJob().getId(), StreamStatusRunState.COMPLETE,
        StreamStatusJobType.SYNC);
  }

  @Test
  void testIncrementalSync() throws Exception {
    testResources.runIncrementalSyncForAWorkspaceId(workspaceId);
  }

  @Test
  @DisabledIfEnvironmentVariable(named = IS_GKE,
                                 matches = TRUE,
                                 disabledReason = "The different way of interacting with the source db causes errors")
  void testMultipleSchemasAndTablesSyncAndReset() throws Exception {
    // create tables in another schema
    // NOTE: this command fails in GKE because we already ran it in a previous test case and we use the
    // same
    // database instance across the test suite. To get it to work, we need to do something better with
    // cleanup.
    testHarness.runSqlScriptInSource("postgres_second_schema_multiple_tables.sql");

    final UUID sourceId = testHarness.createPostgresSource().getSourceId();
    final UUID destinationId = testHarness.createPostgresDestination().getDestinationId();
    final SourceDiscoverSchemaRead discoverResult = testHarness.discoverSourceSchemaWithId(sourceId);
    final AirbyteCatalog catalog = discoverResult.getCatalog();

    final SyncMode srcSyncMode = SyncMode.FULL_REFRESH;
    final DestinationSyncMode dstSyncMode = DestinationSyncMode.OVERWRITE;
    catalog.getStreams().forEach(s -> s.getConfig().syncMode(srcSyncMode).selected(true).destinationSyncMode(dstSyncMode));
    final var conn =
        testHarness.createConnectionSourceNamespace(new TestConnectionCreate.Builder(
            sourceId,
            destinationId,
            catalog,
            discoverResult.getCatalogId())
                .build());

    final var connectionId = conn.getConnectionId();
    final JobInfoRead connectionSyncRead = testHarness.syncConnection(connectionId);
    testHarness.waitForSuccessfulJob(connectionSyncRead.getJob());

    Asserts.assertSourceAndDestinationDbRawRecordsInSync(
        testHarness.getSourceDatabase(), testHarness.getDestinationDatabase(), PUBLIC_SCHEMA_NAME,
        conn.getNamespaceFormat().replace("${SOURCE_NAMESPACE}", PUBLIC), false,
        WITHOUT_SCD_TABLE);
    Asserts.assertSourceAndDestinationDbRawRecordsInSync(
        testHarness.getSourceDatabase(), testHarness.getDestinationDatabase(), "staging",
        conn.getNamespaceFormat().replace("${SOURCE_NAMESPACE}", "staging"), false, false);
    final JobInfoRead connectionResetRead = testHarness.resetConnection(connectionId);
    testHarness.waitForSuccessfulJob(connectionResetRead.getJob());
    assertDestinationDbEmpty(testHarness.getDestinationDatabase());
  }

  // TODO (Angel): Enable once we fix the docker compose tests
  @Test
  @EnabledIfEnvironmentVariable(named = KUBE,
                                matches = TRUE)
  @DisabledIfEnvironmentVariable(named = IS_GKE,
                                 matches = TRUE,
                                 disabledReason = SLOW_TEST_IN_GKE)
  void testPartialResetResetAllWhenSchemaIsModified(final TestInfo testInfo) throws Exception {
    LOGGER.info("Running: " + testInfo.getDisplayName());

    // Add Table
    final String additionalTable = "additional_table";
    final Database sourceDb = testHarness.getSourceDatabase();
    sourceDb.query(ctx -> {
      ctx.createTableIfNotExists(additionalTable)
          .columns(DSL.field("id", SQLDataType.INTEGER), DSL.field(FIELD, SQLDataType.VARCHAR)).execute();
      ctx.truncate(additionalTable).execute();
      ctx.insertInto(DSL.table(additionalTable)).columns(DSL.field("id"), DSL.field(FIELD)).values(1,
          "1").execute();
      ctx.insertInto(DSL.table(additionalTable)).columns(DSL.field("id"), DSL.field(FIELD)).values(2,
          "2").execute();
      return null;
    });
    UUID sourceId = testHarness.createPostgresSource().getSourceId();
    final SourceDiscoverSchemaRead discoverResult = testHarness.discoverSourceSchemaWithId(sourceId);
    final AirbyteCatalog catalog = discoverResult.getCatalog();
    final UUID destinationId = testHarness.createPostgresDestination().getDestinationId();
    final OperationRead operation = testHarness.createNormalizationOperation();

    catalog.getStreams().forEach(s -> s.getConfig().selected(true));
    testHarness.setIncrementalAppendSyncMode(catalog, List.of(COLUMN_ID));

    final ConnectionRead connection =
        testHarness.createConnection(new TestConnectionCreate.Builder(
            sourceId,
            destinationId,
            catalog,
            discoverResult.getCatalogId())
                .build());

    // Run initial sync
    final JobInfoRead syncRead = testHarness.syncConnection(connection.getConnectionId());
    testHarness.waitForSuccessfulJob(syncRead.getJob());

    Asserts.assertSourceAndDestinationDbRawRecordsInSync(
        testHarness.getSourceDatabase(), testHarness.getDestinationDatabase(), PUBLIC_SCHEMA_NAME,
        connection.getNamespaceFormat(), false, WITHOUT_SCD_TABLE);
    Asserts.assertStreamStateContainsStream(testHarness, connection.getConnectionId(), List.of(
        new StreamDescriptor().name(ID_AND_NAME).namespace(PUBLIC),
        new StreamDescriptor().name(additionalTable).namespace(PUBLIC)));

    LOGGER.info("Initial sync ran, now running an update with a stream being removed.");

    /*
     * Remove stream
     */
    sourceDb.query(ctx -> ctx.dropTableIfExists(additionalTable).execute());

    // Update with refreshed catalog
    AirbyteCatalog refreshedCatalog = testHarness.discoverSourceSchemaWithoutCache(sourceId);
    refreshedCatalog.getStreams().forEach(s -> s.getConfig().selected(true));
    WebBackendConnectionUpdate update = testHarness.getUpdateInput(connection, refreshedCatalog,
        operation);
    testHarness.webBackendUpdateConnection(update);

    // Wait until the sync from the UpdateConnection is finished
    final JobRead syncFromTheUpdate1 =
        testHarness.waitUntilTheNextJobIsStarted(connection.getConnectionId(),
            syncRead.getJob().getId());
    testHarness.waitForSuccessfulJob(syncFromTheUpdate1);

    // We do not check that the source and the dest are in sync here because removing a stream
    // doesn't remove that
    Asserts.assertStreamStateContainsStream(testHarness, connection.getConnectionId(), List.of(
        new StreamDescriptor().name(ID_AND_NAME).namespace(PUBLIC)));

    LOGGER.info("Remove done, now running an update with a stream being added.");

    /*
     * Add a stream -- the value of in the table are different than the initial import to ensure that it
     * is properly reset.
     */
    sourceDb.query(ctx -> {
      ctx.createTableIfNotExists(additionalTable)
          .columns(DSL.field("id", SQLDataType.INTEGER), DSL.field(FIELD, SQLDataType.VARCHAR)).execute();
      ctx.truncate(additionalTable).execute();
      ctx.insertInto(DSL.table(additionalTable)).columns(DSL.field("id"), DSL.field(FIELD)).values(3,
          "3").execute();
      ctx.insertInto(DSL.table(additionalTable)).columns(DSL.field("id"), DSL.field(FIELD)).values(4,
          "4").execute();
      return null;
    });

    sourceId = testHarness.createPostgresSource().getSourceId();
    refreshedCatalog = testHarness.discoverSourceSchema(sourceId);
    refreshedCatalog.getStreams().forEach(s -> s.getConfig().selected(true));
    update = testHarness.getUpdateInput(connection, refreshedCatalog, operation);
    testHarness.webBackendUpdateConnection(update);

    final JobRead syncFromTheUpdate2 =
        testHarness.waitUntilTheNextJobIsStarted(connection.getConnectionId(),
            syncFromTheUpdate1.getId());
    testHarness.waitForSuccessfulJob(syncFromTheUpdate2);

    // We do not check that the source and the dest are in sync here because removing a stream
    // doesn't remove that
    Asserts.assertSourceAndDestinationDbRawRecordsInSync(
        testHarness.getSourceDatabase(), testHarness.getDestinationDatabase(), PUBLIC_SCHEMA_NAME,
        connection.getNamespaceFormat(), true, WITHOUT_SCD_TABLE);
    Asserts.assertStreamStateContainsStream(testHarness, connection.getConnectionId(), List.of(
        new StreamDescriptor().name(ID_AND_NAME).namespace(PUBLIC),
        new StreamDescriptor().name(additionalTable).namespace(PUBLIC)));

    LOGGER.info("Addition done, now running an update with a stream being updated.");

    // Update
    sourceDb.query(ctx -> {
      ctx.dropTableIfExists(additionalTable).execute();
      ctx.createTableIfNotExists(additionalTable)
          .columns(DSL.field("id", SQLDataType.INTEGER), DSL.field(FIELD, SQLDataType.VARCHAR),
              DSL.field("another_field", SQLDataType.VARCHAR))
          .execute();
      ctx.truncate(additionalTable).execute();
      ctx.insertInto(DSL.table(additionalTable)).columns(DSL.field("id"), DSL.field(FIELD),
          DSL.field("another_field")).values(3, "3", "three")
          .execute();
      ctx.insertInto(DSL.table(additionalTable)).columns(DSL.field("id"), DSL.field(FIELD),
          DSL.field("another_field")).values(4, "4", "four")
          .execute();
      return null;
    });

    sourceId = testHarness.createPostgresSource().getSourceId();
    refreshedCatalog = testHarness.discoverSourceSchema(sourceId);
    refreshedCatalog.getStreams().forEach(s -> s.getConfig().selected(true));
    update = testHarness.getUpdateInput(connection, refreshedCatalog, operation);
    testHarness.webBackendUpdateConnection(update);

    final JobRead syncFromTheUpdate3 =
        testHarness.waitUntilTheNextJobIsStarted(connection.getConnectionId(),
            syncFromTheUpdate2.getId());
    testHarness.waitForSuccessfulJob(syncFromTheUpdate3);

    // We do not check that the source and the dest are in sync here because removing a stream
    // doesn't remove that
    Asserts.assertSourceAndDestinationDbRawRecordsInSync(
        testHarness.getSourceDatabase(), testHarness.getDestinationDatabase(), PUBLIC_SCHEMA_NAME,
        connection.getNamespaceFormat(), true, WITHOUT_SCD_TABLE);
    Asserts.assertStreamStateContainsStream(testHarness, connection.getConnectionId(), List.of(
        new StreamDescriptor().name(ID_AND_NAME).namespace(PUBLIC),
        new StreamDescriptor().name(additionalTable).namespace(PUBLIC)));
  }

  @Test
  void testIncrementalDedupeSyncRemoveOneColumn() throws Exception {
    final UUID sourceId = testHarness.createPostgresSource().getSourceId();
    final UUID destinationId = testHarness.createPostgresDestination().getDestinationId();
    final UUID normalizationOpId = testHarness.createNormalizationOperation().getOperationId();
    final SourceDiscoverSchemaRead discoverResult = testHarness.discoverSourceSchemaWithId(sourceId);
    final AirbyteCatalog catalog = discoverResult.getCatalog();
    final SyncMode srcSyncMode = SyncMode.INCREMENTAL;
    final DestinationSyncMode dstSyncMode = DestinationSyncMode.APPEND_DEDUP;
    catalog.getStreams().forEach(s -> s.getConfig()
        .selected(true)
        .syncMode(srcSyncMode)
        .cursorField(List.of(COLUMN_ID))
        .destinationSyncMode(dstSyncMode)
        .primaryKey(List.of(List.of(COLUMN_ID))));
    final var conn =
        testHarness.createConnection(new TestConnectionCreate.Builder(
            sourceId,
            destinationId,
            catalog,
            discoverResult.getCatalogId())
                .setNormalizationOperationId(normalizationOpId)
                .build());
    final var connectionId = conn.getConnectionId();
    // sync from start
    LOGGER.info("First incremental sync");
    final JobInfoRead connectionSyncRead1 = testHarness.syncConnection(connectionId);
    testHarness.waitForSuccessfulJob(connectionSyncRead1.getJob());
    LOGGER.info("state after sync: {}", testHarness.getConnectionState(connectionId));

    final var dst = testHarness.getDestinationDatabase();
    Asserts.assertSourceAndDestinationDbRawRecordsInSync(testHarness.getSourceDatabase(), dst, PUBLIC_SCHEMA_NAME, conn.getNamespaceFormat(), true,
        WITH_SCD_TABLE);

    // Update the catalog, so we only select the id column.
    catalog.getStreams().get(0).getConfig().fieldSelectionEnabled(true).addSelectedFieldsItem(new SelectedFieldInfo().addFieldPathItem("id"));
    testHarness.updateConnectionCatalog(connectionId, catalog);

    // add new records and run again.
    LOGGER.info("Adding new records to source database");
    final Database source = testHarness.getSourceDatabase();
    final List<JsonNode> expectedRawRecords = testHarness.retrieveRecordsFromDatabase(source, STREAM_NAME);
    source.query(ctx -> ctx.execute("INSERT INTO id_and_name(id, name) VALUES(6, 'mike')"));
    source.query(ctx -> ctx.execute("INSERT INTO id_and_name(id, name) VALUES(7, 'chris')"));
    // The expected new raw records should only have the ID column.
    expectedRawRecords.add(Jsons.jsonNode(ImmutableMap.builder().put(COLUMN_ID, 6).build()));
    expectedRawRecords.add(Jsons.jsonNode(ImmutableMap.builder().put(COLUMN_ID, 7).build()));
    final JobInfoRead connectionSyncRead2 = testHarness.syncConnection(connectionId);
    LOGGER.info("Running second sync: job {} with status {}", connectionSyncRead2.getJob().getId(), connectionSyncRead2.getJob().getStatus());
    testHarness.waitForSuccessfulJob(connectionSyncRead2.getJob());
    LOGGER.info("state after sync: {}", testHarness.getConnectionState(connectionId));

    // For the normalized records, they should all only have the ID column.
    final List<JsonNode> expectedNormalizedRecords = testHarness.retrieveRecordsFromDatabase(source, STREAM_NAME).stream()
        .map((record) -> ((ObjectNode) record).retain(COLUMN_ID)).collect(Collectors.toList());
    Asserts.assertRawDestinationContains(dst, expectedRawRecords, conn.getNamespaceFormat(), STREAM_NAME);
    testHarness.assertNormalizedDestinationContainsIdColumn(conn.getNamespaceFormat(), expectedNormalizedRecords);
  }

  static void assertDestinationDbEmpty(final Database dst) throws Exception {
    final Set<SchemaTableNamePair> destinationTables = Databases.listAllTables(dst);

    for (final SchemaTableNamePair pair : destinationTables) {
      final List<JsonNode> recs = Databases.retrieveRecordsFromDatabase(dst, pair.getFullyQualifiedTableName());
      Assertions.assertTrue(recs.isEmpty());
    }
  }

}
