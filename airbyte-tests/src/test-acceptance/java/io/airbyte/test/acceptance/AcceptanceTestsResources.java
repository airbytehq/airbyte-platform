/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.acceptance;

import static io.airbyte.commons.ConstantsKt.DEFAULT_ORGANIZATION_ID;
import static io.airbyte.test.utils.AcceptanceTestUtils.createAirbyteApiClient;
import static io.airbyte.test.utils.AcceptanceTestUtils.modifyCatalog;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.model.generated.AirbyteCatalog;
import io.airbyte.api.client.model.generated.AirbyteStream;
import io.airbyte.api.client.model.generated.ConnectionScheduleData;
import io.airbyte.api.client.model.generated.ConnectionScheduleDataBasicSchedule;
import io.airbyte.api.client.model.generated.ConnectionScheduleDataBasicSchedule.TimeUnit;
import io.airbyte.api.client.model.generated.DestinationDefinitionIdRequestBody;
import io.airbyte.api.client.model.generated.DestinationDefinitionRead;
import io.airbyte.api.client.model.generated.DestinationSyncMode;
import io.airbyte.api.client.model.generated.JobInfoRead;
import io.airbyte.api.client.model.generated.JobRead;
import io.airbyte.api.client.model.generated.JobStatus;
import io.airbyte.api.client.model.generated.SourceDefinitionIdRequestBody;
import io.airbyte.api.client.model.generated.SourceDefinitionRead;
import io.airbyte.api.client.model.generated.SourceDiscoverSchemaRead;
import io.airbyte.api.client.model.generated.StreamStatusJobType;
import io.airbyte.api.client.model.generated.StreamStatusRunState;
import io.airbyte.api.client.model.generated.SyncMode;
import io.airbyte.api.client.model.generated.WorkspaceCreate;
import io.airbyte.commons.json.Jsons;
import io.airbyte.db.Database;
import io.airbyte.featureflag.tests.TestFlagsSetter;
import io.airbyte.test.utils.AcceptanceTestHarness;
import io.airbyte.test.utils.Asserts;
import io.airbyte.test.utils.TestConnectionCreate.Builder;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains all the fixtures, setup, teardown, special assertion logic, etc. for running basic
 * acceptance tests. This can be leveraged by different test classes such that individual test
 * suites can be enabled / disabled / configured as desired. This was extracted from
 * BasicAcceptanceTests and can be further broken up / refactored as necessary.
 */
public class AcceptanceTestsResources {

  private static final Logger LOGGER = LoggerFactory.getLogger(AcceptanceTestsResources.class);

  static final Boolean WITH_SCD_TABLE = true;
  static final Boolean WITHOUT_SCD_TABLE = false;
  static final String GATEWAY_AUTH_HEADER = "X-Endpoint-API-UserInfo";
  // NOTE: this is just a base64 encoding of a jwt representing a test user in some deployments.
  static final String CLOUD_API_USER_HEADER_VALUE = "eyJ1c2VyX2lkIjogImNsb3VkLWFwaSIsICJlbWFpbF92ZXJpZmllZCI6ICJ0cnVlIn0K";
  static final String AIRBYTE_ACCEPTANCE_TEST_WORKSPACE_ID = "AIRBYTE_ACCEPTANCE_TEST_WORKSPACE_ID";
  static final String AIRBYTE_SERVER_HOST = Optional.ofNullable(System.getenv("AIRBYTE_SERVER_HOST")).orElse("http://localhost:8001");
  static final UUID POSTGRES_SOURCE_DEF_ID = UUID.fromString("decd338e-5647-4c0b-adf4-da0e75f5a750");
  static final UUID POSTGRES_DEST_DEF_ID = UUID.fromString("25c5221d-dce2-4163-ade9-739ef790f503");
  public static final String KUBE = "KUBE";
  public static final String TRUE = "true";
  static final String DISABLE_TEMPORAL_TESTS_IN_GKE =
      "Test disabled because it specifically interacts with Temporal, which is deployment-dependent ";
  public static final int JITTER_MAX_INTERVAL_SECS = 10;
  public static final int FINAL_INTERVAL_SECS = 60;
  public static final int MAX_TRIES = 3;
  static final String STATE_AFTER_SYNC_ONE = "state after sync 1: {}";
  static final String STATE_AFTER_SYNC_TWO = "state after sync 2: {}";
  static final String GERALT = "geralt";
  static final int MAX_SCHEDULED_JOB_RETRIES = 10;

  private AcceptanceTestHarness testHarness;
  private UUID workspaceId;

  private final ConnectionScheduleData basicScheduleData;

  public AcceptanceTestHarness getTestHarness() {
    return testHarness;
  }

  public UUID getWorkspaceId() {
    return workspaceId;
  }

  public ConnectionScheduleData getBasicScheduleData() {
    return basicScheduleData;
  }

  public AcceptanceTestsResources() {
    this.basicScheduleData = new ConnectionScheduleData(
        new ConnectionScheduleDataBasicSchedule(TimeUnit.HOURS, 1L),
        null);
  }

  /**
   * Waits for the given connection to finish, waiting at 30s intervals, until maxRetries is reached.
   *
   * @param jobRead the job to wait for
   * @throws InterruptedException exception if interrupted while waiting
   */
  void waitForSuccessfulJobWithRetries(final JobRead jobRead) throws InterruptedException {
    int i;
    for (i = 0; i < MAX_SCHEDULED_JOB_RETRIES; i++) {
      try {
        testHarness.waitForSuccessfulJob(jobRead);
        break;
      } catch (final Exception e) {
        LOGGER.info("Something went wrong querying jobs API, retrying...");
      }
      Thread.sleep(Duration.ofSeconds(30).toMillis());
    }

    if (i == MAX_SCHEDULED_JOB_RETRIES) {
      LOGGER.error("Sync job did not complete within 5 minutes");
    }
  }

  void runIncrementalSyncForAWorkspaceId(final UUID workspaceId) throws Exception {
    LOGGER.info("Starting testIncrementalSync()");
    final UUID sourceId = testHarness.createPostgresSource(workspaceId).getSourceId();
    final UUID destinationId = testHarness.createPostgresDestination(workspaceId).getDestinationId();
    final SourceDiscoverSchemaRead discoverResult = testHarness.discoverSourceSchemaWithId(sourceId);
    final AirbyteCatalog retrievedCatalog = discoverResult.getCatalog();
    final AirbyteStream stream = retrievedCatalog.getStreams().get(0).getStream();

    assertEquals(Lists.newArrayList(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL), stream.getSupportedSyncModes());
    Assertions.assertFalse(stream.getSourceDefinedCursor());
    assertTrue(stream.getDefaultCursorField().isEmpty());
    assertTrue(stream.getSourceDefinedPrimaryKey().isEmpty());

    final SyncMode srcSyncMode = SyncMode.INCREMENTAL;
    final DestinationSyncMode dstSyncMode = DestinationSyncMode.APPEND;
    final AirbyteCatalog catalog = modifyCatalog(
        retrievedCatalog,
        Optional.of(srcSyncMode),
        Optional.of(dstSyncMode),
        Optional.of(List.of(AcceptanceTestHarness.COLUMN_ID)),
        Optional.empty(),
        Optional.of(true),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
    final var conn =
        testHarness.createConnection(new Builder(
            sourceId,
            destinationId,
            catalog,
            discoverResult.getCatalogId(),
            testHarness.getDataplaneGroupId())
                .build());
    LOGGER.info("Beginning testIncrementalSync() sync 1");

    final var connectionId = conn.getConnectionId();
    final JobInfoRead connectionSyncRead1 = testHarness.syncConnection(connectionId);
    testHarness.waitForSuccessfulJob(connectionSyncRead1.getJob());

    LOGGER.info(STATE_AFTER_SYNC_ONE, testHarness.getConnectionState(connectionId));

    final var src = testHarness.getSourceDatabase();
    final var dst = testHarness.getDestinationDatabase();
    Asserts.assertSourceAndDestinationDbRawRecordsInSync(src, dst, conn.getNamespaceFormat(), AcceptanceTestHarness.PUBLIC_SCHEMA_NAME, false,
        WITHOUT_SCD_TABLE);
    Asserts.assertStreamStatuses(testHarness, workspaceId, connectionId, connectionSyncRead1.getJob().getId(), StreamStatusRunState.COMPLETE,
        StreamStatusJobType.SYNC);

    // add new records and run again.
    final Database source = testHarness.getSourceDatabase();
    // get contents of source before mutating records.
    final List<JsonNode> expectedRecords = testHarness.retrieveRecordsFromDatabase(source, AcceptanceTestHarness.STREAM_NAME);
    expectedRecords.add(
        Jsons.jsonNode(ImmutableMap.builder().put(AcceptanceTestHarness.COLUMN_ID, 6).put(AcceptanceTestHarness.COLUMN_NAME, GERALT).build()));
    // add a new record
    source.query(ctx -> ctx.execute("INSERT INTO id_and_name(id, name) VALUES(6, 'geralt')"));
    // mutate a record that was already synced with out updating its cursor value. if we are actually
    // full refreshing, this record will appear in the output and cause the test to fail. if we are,
    // correctly, doing incremental, we will not find this value in the destination.
    source.query(ctx -> ctx.execute("UPDATE id_and_name SET name='yennefer' WHERE id=2"));

    LOGGER.info("Starting testIncrementalSync() sync 2");
    final JobInfoRead connectionSyncRead2 = testHarness.syncConnection(connectionId);
    testHarness.waitForSuccessfulJob(connectionSyncRead2.getJob());

    LOGGER.info(STATE_AFTER_SYNC_TWO, testHarness.getConnectionState(connectionId));

    Asserts.assertRawDestinationContains(dst, expectedRecords, conn.getNamespaceFormat(), AcceptanceTestHarness.STREAM_NAME);
    Asserts.assertStreamStatuses(testHarness, workspaceId, connectionId, connectionSyncRead2.getJob().getId(), StreamStatusRunState.COMPLETE,
        StreamStatusJobType.SYNC);

    // reset back to no data.

    LOGGER.info("Starting testIncrementalSync() reset");
    final JobInfoRead jobInfoRead = testHarness.resetConnection(connectionId);
    testHarness.waitWhileJobHasStatus(jobInfoRead.getJob(),
        Sets.newHashSet(JobStatus.PENDING, JobStatus.RUNNING, JobStatus.INCOMPLETE, JobStatus.FAILED));
    // This is a band-aid to prevent some race conditions where the job status was updated but we may
    // still be cleaning up some data in the reset table. This would be an argument for reworking the
    // source of truth of the replication workflow state to be in DB rather than in Memory and
    // serialized automagically by temporal
    testHarness.waitWhileJobIsRunning(jobInfoRead.getJob(), Duration.ofMinutes(1));

    LOGGER.info("state after reset: {}", testHarness.getConnectionState(connectionId));
    // TODO enable once stream status for resets has been fixed
    // testHarness.assertStreamStatuses(workspaceId, connectionId, StreamStatusRunState.COMPLETE,
    // StreamStatusJobType.RESET);

    // NOTE: this is a weird usage of retry policy, but we've seen flakes where the destination still
    // has records even though the reset job is successful.
    Failsafe.with(RetryPolicy.builder()
        .withBackoff(Duration.ofSeconds(10), Duration.ofSeconds(600))
        .withMaxRetries(4)
        .build())
        .run(() -> Asserts.assertRawDestinationContains(dst, Collections.emptyList(), conn.getNamespaceFormat(), AcceptanceTestHarness.STREAM_NAME));

    // sync one more time. verify it is the equivalent of a full refresh.
    LOGGER.info("Starting testIncrementalSync() sync 3");
    final JobInfoRead connectionSyncRead3 = testHarness.syncConnection(connectionId);
    testHarness.waitForSuccessfulJob(connectionSyncRead3.getJob());

    LOGGER.info("state after sync 3: {}", testHarness.getConnectionState(connectionId));

    Asserts.assertSourceAndDestinationDbRawRecordsInSync(src, dst, conn.getNamespaceFormat(), AcceptanceTestHarness.PUBLIC_SCHEMA_NAME, false,
        WITHOUT_SCD_TABLE);
    Asserts.assertStreamStatuses(testHarness, workspaceId, connectionId, connectionSyncRead3.getJob().getId(), StreamStatusRunState.COMPLETE,
        StreamStatusJobType.SYNC);
  }

  record SyncIds(UUID connectionId, Long jobId, Integer attemptNumber) {}

  SyncIds runSmallSyncForAWorkspaceId(final UUID workspaceId) throws Exception {
    LOGGER.info("Starting runSmallSyncForAWorkspaceId(" + workspaceId + ")");
    final UUID sourceId = testHarness.createPostgresSource(workspaceId).getSourceId();
    final UUID destinationId = testHarness.createPostgresDestination(workspaceId).getDestinationId();
    final SourceDiscoverSchemaRead discoverResult = testHarness.discoverSourceSchemaWithId(sourceId);
    final AirbyteCatalog retrievedCatalog = discoverResult.getCatalog();
    final AirbyteStream stream = retrievedCatalog.getStreams().get(0).getStream();

    assertEquals(Lists.newArrayList(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL), stream.getSupportedSyncModes());
    Assertions.assertFalse(stream.getSourceDefinedCursor());
    assertTrue(stream.getDefaultCursorField().isEmpty());
    assertTrue(stream.getSourceDefinedPrimaryKey().isEmpty());

    final SyncMode srcSyncMode = SyncMode.INCREMENTAL;
    final DestinationSyncMode dstSyncMode = DestinationSyncMode.APPEND;
    final AirbyteCatalog catalog = modifyCatalog(
        retrievedCatalog,
        Optional.of(srcSyncMode),
        Optional.of(dstSyncMode),
        Optional.of(List.of(AcceptanceTestHarness.COLUMN_ID)),
        Optional.empty(),
        Optional.of(true),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
    final var conn =
        testHarness.createConnection(new Builder(
            sourceId,
            destinationId,
            catalog,
            discoverResult.getCatalogId(),
            testHarness.getDataplaneGroupId())
                .build());
    LOGGER.info("Beginning runSmallSyncForAWorkspaceId() sync");

    final var connectionId = conn.getConnectionId();
    final JobInfoRead connectionSyncRead1 = testHarness.syncConnection(connectionId);
    testHarness.waitForSuccessfulJob(connectionSyncRead1.getJob());

    LOGGER.info(STATE_AFTER_SYNC_ONE, testHarness.getConnectionState(connectionId));

    final var src = testHarness.getSourceDatabase();
    final var dst = testHarness.getDestinationDatabase();
    Asserts.assertSourceAndDestinationDbRawRecordsInSync(src, dst, conn.getNamespaceFormat(), AcceptanceTestHarness.PUBLIC_SCHEMA_NAME, false,
        WITHOUT_SCD_TABLE);
    Asserts.assertStreamStatuses(testHarness, workspaceId, connectionId, connectionSyncRead1.getJob().getId(), StreamStatusRunState.COMPLETE,
        StreamStatusJobType.SYNC);

    // Assert that job logs exist
    final var jobId = connectionSyncRead1.getJob().getId();
    final var attemptId = connectionSyncRead1.getAttempts().size() - 1;
    testHarness.validateLogs(jobId, attemptId);

    return new SyncIds(connectionId, jobId, attemptId);
  }

  void init() throws URISyntaxException, IOException, InterruptedException, GeneralSecurityException {
    final AirbyteApiClient airbyteApiClient =
        createAirbyteApiClient(AIRBYTE_SERVER_HOST + "/api",
            Map.of(GATEWAY_AUTH_HEADER, CLOUD_API_USER_HEADER_VALUE));
    final TestFlagsSetter testFlagsSetter = new TestFlagsSetter(AIRBYTE_SERVER_HOST);

    // If a workspace id is passed, use that. Otherwise, create a new workspace.
    // NOTE: we want to sometimes use a pre-configured workspace e.g., if we run against a production
    // deployment where we don't want to create workspaces.
    // NOTE: the API client can't create workspaces in GKE deployments, so we need to provide a
    // workspace ID in that environment.
    workspaceId = System.getenv(AIRBYTE_ACCEPTANCE_TEST_WORKSPACE_ID) == null ? airbyteApiClient.getWorkspaceApi()
        .createWorkspace(
            new WorkspaceCreate(
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
        .getWorkspaceId() : UUID.fromString(System.getenv(AIRBYTE_ACCEPTANCE_TEST_WORKSPACE_ID));
    LOGGER.info("workspaceId = {}", workspaceId);

    // log which connectors are being used.
    final SourceDefinitionRead sourceDef =
        airbyteApiClient.getSourceDefinitionApi().getSourceDefinition(new SourceDefinitionIdRequestBody(POSTGRES_SOURCE_DEF_ID));
    final DestinationDefinitionRead destinationDef =
        airbyteApiClient.getDestinationDefinitionApi().getDestinationDefinition(new DestinationDefinitionIdRequestBody(POSTGRES_DEST_DEF_ID));
    LOGGER.info("pg source definition: {}", sourceDef.getDockerImageTag());
    LOGGER.info("pg destination definition: {}", destinationDef.getDockerImageTag());

    testHarness = new AcceptanceTestHarness(airbyteApiClient, workspaceId, testFlagsSetter);

    testHarness.ensureCleanSlate();
  }

  void end() {
    LOGGER.debug("Executing test suite teardown");
    testHarness.stopDbAndContainers();
  }

  void setup() throws SQLException, URISyntaxException, IOException {
    LOGGER.debug("Executing test case setup");
    testHarness.setup();
  }

  void tearDown() {
    LOGGER.debug("Executing test case teardown");
    testHarness.cleanup();
  }

}
