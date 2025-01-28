/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.acceptance;

import static io.airbyte.test.acceptance.AcceptanceTestsResources.FINAL_INTERVAL_SECS;
import static io.airbyte.test.acceptance.AcceptanceTestsResources.JITTER_MAX_INTERVAL_SECS;
import static io.airbyte.test.acceptance.AcceptanceTestsResources.MAX_TRIES;
import static io.airbyte.test.acceptance.AcceptanceTestsResources.TRUE;
import static io.airbyte.test.acceptance.AcceptanceTestsResources.WITHOUT_SCD_TABLE;
import static io.airbyte.test.utils.AcceptanceTestHarness.PUBLIC;
import static io.airbyte.test.utils.AcceptanceTestHarness.PUBLIC_SCHEMA_NAME;
import static io.airbyte.test.utils.AcceptanceTestUtils.IS_GKE;
import static io.airbyte.test.utils.AcceptanceTestUtils.modifyCatalog;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airbyte.api.client.model.generated.AirbyteCatalog;
import io.airbyte.api.client.model.generated.CheckConnectionRead;
import io.airbyte.api.client.model.generated.ConnectionScheduleData;
import io.airbyte.api.client.model.generated.ConnectionScheduleDataCron;
import io.airbyte.api.client.model.generated.ConnectionScheduleType;
import io.airbyte.api.client.model.generated.DestinationSyncMode;
import io.airbyte.api.client.model.generated.JobInfoRead;
import io.airbyte.api.client.model.generated.JobRead;
import io.airbyte.api.client.model.generated.JobStatus;
import io.airbyte.api.client.model.generated.SourceDefinitionRead;
import io.airbyte.api.client.model.generated.SourceDiscoverSchemaRead;
import io.airbyte.api.client.model.generated.SourceRead;
import io.airbyte.api.client.model.generated.StreamStatusJobType;
import io.airbyte.api.client.model.generated.StreamStatusRunState;
import io.airbyte.api.client.model.generated.SyncMode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.db.Database;
import io.airbyte.test.utils.AcceptanceTestHarness;
import io.airbyte.test.utils.Asserts;
import io.airbyte.test.utils.Databases;
import io.airbyte.test.utils.SchemaTableNamePair;
import io.airbyte.test.utils.TestConnectionCreate;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
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
 */
@SuppressWarnings({"PMD.JUnitTestsShouldIncludeAssert", "SqlDialectInspection",
  "SqlNoDataSourceInspection",
  "PMD.AvoidDuplicateLiterals"})
@Execution(ExecutionMode.CONCURRENT)
@Tag("sync")
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
  void testSourceCheckConnection() throws IOException {
    final UUID sourceId = testHarness.createPostgresSource().getSourceId();

    final CheckConnectionRead checkConnectionRead = testHarness.checkSource(sourceId);

    assertEquals(
        CheckConnectionRead.Status.SUCCEEDED,
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
    final SyncMode srcSyncMode = SyncMode.FULL_REFRESH;
    final DestinationSyncMode dstSyncMode = DestinationSyncMode.OVERWRITE;
    final AirbyteCatalog catalog = modifyCatalog(
        discoverResult.getCatalog(),
        Optional.of(srcSyncMode),
        Optional.of(dstSyncMode),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
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
    final SyncMode srcSyncMode = SyncMode.FULL_REFRESH;
    final DestinationSyncMode dstSyncMode = DestinationSyncMode.OVERWRITE;
    final AirbyteCatalog catalog = modifyCatalog(
        discoverResult.getCatalog(),
        Optional.of(srcSyncMode),
        Optional.of(dstSyncMode),
        Optional.empty(),
        Optional.empty(),
        Optional.of(true),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());

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

    // NOTE: this cron should run once every two minutes.
    final ConnectionScheduleData connectionScheduleData = new ConnectionScheduleData(null,
        new ConnectionScheduleDataCron("0 */2 * * * ?", "UTC"));
    final SyncMode srcSyncMode = SyncMode.FULL_REFRESH;
    final DestinationSyncMode dstSyncMode = DestinationSyncMode.OVERWRITE;
    final AirbyteCatalog catalog = modifyCatalog(
        discoverResult.getCatalog(),
        Optional.of(srcSyncMode),
        Optional.of(dstSyncMode),
        Optional.empty(),
        Optional.empty(),
        Optional.of(true),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
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

    // NOTE: this is an unusual use of a retry policy. Sometimes the raw tables haven't been cleaned up
    // even though the job
    // is marked successful.
    final String retryAssertOutcome = Failsafe.with(RetryPolicy.builder()
        .withBackoff(Duration.ofSeconds(JITTER_MAX_INTERVAL_SECS), Duration.ofSeconds(FINAL_INTERVAL_SECS))
        .withMaxRetries(MAX_TRIES)
        .build()).get(() -> {
          Asserts.assertSourceAndDestinationDbRawRecordsInSync(testHarness.getSourceDatabase(), testHarness.getDestinationDatabase(),
              PUBLIC_SCHEMA_NAME,
              conn.getNamespaceFormat(),
              false, WITHOUT_SCD_TABLE);
          return "success"; // If the assertion throws after all the retries, then retryWithJitter will return null.
        });
    assertEquals("success", retryAssertOutcome);

    Asserts.assertStreamStatuses(testHarness, workspaceId, connectionId, jobRead.getId(), StreamStatusRunState.COMPLETE, StreamStatusJobType.SYNC);

    testHarness.deleteConnection(connectionId);

    // remove connection to avoid exception during tear down
    testHarness.removeConnection(connectionId);
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
    final SyncMode srcSyncMode = SyncMode.FULL_REFRESH;
    final DestinationSyncMode dstSyncMode = DestinationSyncMode.OVERWRITE;
    final AirbyteCatalog catalog = modifyCatalog(
        discoverResult.getCatalog(),
        Optional.of(srcSyncMode),
        Optional.of(dstSyncMode),
        Optional.empty(),
        Optional.empty(),
        Optional.of(true),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
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

  static void assertDestinationDbEmpty(final Database dst) throws Exception {
    final Set<SchemaTableNamePair> destinationTables = Databases.listAllTables(dst);

    for (final SchemaTableNamePair pair : destinationTables) {
      final List<JsonNode> recs = Databases.retrieveRecordsFromDatabase(dst, pair.getFullyQualifiedTableName());
      Assertions.assertTrue(recs.isEmpty());
    }
  }

}
