/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.acceptance

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.collect.ImmutableMap
import com.google.common.collect.Lists
import com.google.common.collect.Sets
import dev.failsafe.Failsafe
import dev.failsafe.RetryPolicy
import dev.failsafe.function.CheckedRunnable
import io.airbyte.api.client.model.generated.ConnectionEventsRequestBody
import io.airbyte.api.client.model.generated.ConnectionScheduleData
import io.airbyte.api.client.model.generated.ConnectionScheduleDataBasicSchedule
import io.airbyte.api.client.model.generated.DestinationDefinitionIdRequestBody
import io.airbyte.api.client.model.generated.DestinationSyncMode
import io.airbyte.api.client.model.generated.JobIdRequestBody
import io.airbyte.api.client.model.generated.JobRead
import io.airbyte.api.client.model.generated.JobStatus
import io.airbyte.api.client.model.generated.SourceDefinitionIdRequestBody
import io.airbyte.api.client.model.generated.StreamStatusJobType
import io.airbyte.api.client.model.generated.StreamStatusRunState
import io.airbyte.api.client.model.generated.SyncMode
import io.airbyte.api.client.model.generated.WorkspaceCreate
import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.commons.json.Jsons
import io.airbyte.featureflag.tests.TestFlagsSetter
import io.airbyte.test.acceptance.SyncAcceptanceTests.Companion.assertDestinationDbEmpty
import io.airbyte.test.utils.AcceptanceTestHarness
import io.airbyte.test.utils.AcceptanceTestUtils.createAirbyteAdminApiClient
import io.airbyte.test.utils.AcceptanceTestUtils.modifyCatalog
import io.airbyte.test.utils.Asserts.assertRawDestinationContains
import io.airbyte.test.utils.Asserts.assertSourceAndDestinationDbRawRecordsInSync
import io.airbyte.test.utils.Asserts.assertStreamStatuses
import io.airbyte.test.utils.TestConnectionCreate
import org.jooq.DSLContext
import org.junit.jupiter.api.Assertions
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException
import java.net.URISyntaxException
import java.security.GeneralSecurityException
import java.sql.SQLException
import java.time.Duration
import java.util.Optional
import java.util.UUID

/**
 * Contains all the fixtures, setup, teardown, special assertion logic, etc. for running basic
 * acceptance tests. This can be leveraged by different test classes such that individual test
 * suites can be enabled / disabled / configured as desired. This was extracted from
 * BasicAcceptanceTests and can be further broken up / refactored as necessary.
 */
class AcceptanceTestsResources {
  lateinit var testHarness: AcceptanceTestHarness
  lateinit var workspaceId: UUID
  val basicScheduleData: ConnectionScheduleData =
    ConnectionScheduleData(
      ConnectionScheduleDataBasicSchedule(
        ConnectionScheduleDataBasicSchedule.TimeUnit.HOURS,
        1L,
      ),
      null,
    )

  /**
   * Waits for the given connection to finish, waiting at 30s intervals, until maxRetries is reached.
   *
   * @param jobRead the job to wait for
   * @throws InterruptedException exception if interrupted while waiting
   */
  @Throws(InterruptedException::class)
  fun waitForSuccessfulJobWithRetries(jobRead: JobRead) {
    var i = 0
    while (i < MAX_SCHEDULED_JOB_RETRIES) {
      try {
        testHarness.waitForSuccessfulJob(jobRead)
        break
      } catch (e: Exception) {
        LOGGER.info("Something went wrong querying jobs API, retrying...")
      }
      Thread.sleep(Duration.ofSeconds(30).toMillis())
      i++
    }

    if (i == MAX_SCHEDULED_JOB_RETRIES) {
      LOGGER.error("Sync job did not complete within 5 minutes")
    }
  }

  @Throws(Exception::class)
  fun runIncrementalSyncForAWorkspaceId(workspaceId: UUID) {
    LOGGER.info("Starting testIncrementalSync()")
    val sourceId = testHarness.createPostgresSource(workspaceId).sourceId
    val destinationId = testHarness.createPostgresDestination(workspaceId).destinationId
    val discoverResult = testHarness.discoverSourceSchemaWithId(sourceId)
    val retrievedCatalog = discoverResult.catalog
    val stream = retrievedCatalog!!.streams[0].stream

    Assertions.assertEquals(Lists.newArrayList(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL), stream!!.supportedSyncModes)
    Assertions.assertFalse(stream.sourceDefinedCursor!!)
    Assertions.assertTrue(stream.defaultCursorField!!.isEmpty())
    Assertions.assertTrue(stream.sourceDefinedPrimaryKey!!.isEmpty())

    val srcSyncMode = SyncMode.INCREMENTAL
    val dstSyncMode = DestinationSyncMode.APPEND
    val catalog =
      modifyCatalog(
        originalCatalog = retrievedCatalog,
        replacementSourceSyncMode = Optional.of(srcSyncMode),
        replacementDestinationSyncMode = Optional.of(dstSyncMode),
        replacementCursorFields = Optional.of(listOf(AcceptanceTestHarness.COLUMN_ID)),
        replacementSelected = Optional.of(true),
      )
    val conn =
      testHarness.createConnection(
        TestConnectionCreate
          .Builder(
            srcId = sourceId,
            dstId = destinationId,
            configuredCatalog = catalog,
            catalogId = discoverResult.catalogId!!,
            dataplaneGroupId = testHarness.dataplaneGroupId,
          ).build(),
      )
    LOGGER.info("Beginning testIncrementalSync() sync 1")

    val connectionId = conn.connectionId
    val connectionSyncRead1 = testHarness.syncConnection(connectionId)
    testHarness.waitForSuccessfulJob(connectionSyncRead1.job)

    LOGGER.info(STATE_AFTER_SYNC_ONE, testHarness.getConnectionState(connectionId))

    // postgres_init.sql inserts 5 records. Assert that we wrote stats correctly.
    // (this is a bit sketchy, in that theoretically the source could emit a state message,
    // then fail an attempt, and a subsequent attempt would then not read all the records.
    // But with just 5 records, that seems unlikely.)
    val lastAttempt =
      testHarness
        .getJobInfoRead(connectionSyncRead1.job.id)
        .attempts
        .last()
        .attempt
    testHarness.apiClient.jobsApi.getJobDebugInfo(JobIdRequestBody(connectionSyncRead1.job.id))
    Assertions.assertAll(
      "totalStats were incorrect",
      { Assertions.assertEquals(5, lastAttempt.totalStats!!.recordsEmitted, "totalStats.recordsEmitted was incorrect") },
      { Assertions.assertEquals(118, lastAttempt.totalStats!!.bytesEmitted, "totalStats.bytesEmitted was incorrect") },
      { Assertions.assertEquals(1, lastAttempt.totalStats!!.stateMessagesEmitted, "totalStats.stateMessagesEmitted was incorrect") },
      // the API doesn't return records/bytes committed on totalStats, so don't assert against them
      // { Assertions.assertEquals(5, lastAttempt.totalStats!!.recordsCommitted, "totalStats.recordsCommitted was incorrect") },
      // { Assertions.assertEquals(118, lastAttempt.totalStats!!.bytesCommitted, "totalStats.bytesCommitted was incorrect") },
    )
    Assertions.assertEquals(1, lastAttempt.streamStats!!.size, "Expected to see stats for exactly one stream. Got ${lastAttempt.streamStats}")
    val lastAttemptStreamStats = lastAttempt.streamStats!!.first()
    Assertions.assertAll(
      "streamStats were incorrect",
      { Assertions.assertEquals("id_and_name", lastAttemptStreamStats.streamName) },
      { Assertions.assertNull(lastAttemptStreamStats.streamNamespace) },
    )
    Assertions.assertAll(
      "streamStats were incorrect",
      { Assertions.assertEquals(5, lastAttemptStreamStats.stats.recordsEmitted, "streamStats.recordsEmitted was incorrect") },
      { Assertions.assertEquals(118, lastAttemptStreamStats.stats.bytesEmitted, "streamStats.bytesEmitted was incorrect") },
      { Assertions.assertEquals(5, lastAttemptStreamStats.stats.recordsCommitted, "streamStats.recordsCommitted was incorrect") },
      // the API doesn't return stateMessagesEmitted / bytesCommitted on streamStats, so don't assert against them
      // { Assertions.assertEquals(1, lastAttemptStreamStats.stats.stateMessagesEmitted, "streamStats.stateMessagesEmitted was incorrect") },
      // { Assertions.assertEquals(118, lastAttemptStreamStats.stats.bytesCommitted, "streamStats.bytesCommitted was incorrect") },
    )
    // this was the only way I found to get to bytesLoaded (conceptually equivalent to bytesCommitted)
    val lastConnectionEventSummary =
      testHarness
        .apiClient
        .connectionApi
        .listConnectionEvents(ConnectionEventsRequestBody(connectionId))
        .events
        .first()
        // summary is declared as Any, so we need to explicitly cast here.
        .summary as Map<String, Int>
    // would you believe "bytesLoaded" isn't declared as a constant anywhere?
    Assertions.assertEquals(118, lastConnectionEventSummary["bytesLoaded"])

    val src = testHarness.getSourceDatabase()
    val dst = testHarness.getDestinationDatabase()
    assertSourceAndDestinationDbRawRecordsInSync(
      source = src,
      destination = dst,
      inputSchema = AcceptanceTestHarness.PUBLIC_SCHEMA_NAME,
      outputSchema = conn.namespaceFormat!!,
      withNormalizedTable = false,
      withScdTable = WITHOUT_SCD_TABLE,
    )
    assertStreamStatuses(
      testHarness = testHarness,
      workspaceId = workspaceId,
      connectionId = connectionId,
      jobId = connectionSyncRead1.job.id,
      expectedRunState = StreamStatusRunState.COMPLETE,
      expectedJobType = StreamStatusJobType.SYNC,
    )

    // add new records and run again.
    val source = testHarness.getSourceDatabase()
    // get contents of source before mutating records.
    val preMutationRecords: MutableList<JsonNode?> =
      testHarness
        .retrieveRecordsFromDatabase(
          source,
          AcceptanceTestHarness.STREAM_NAME,
        ).toMutableList()
    preMutationRecords.add(
      Jsons.jsonNode<ImmutableMap<Any, Any>>(
        ImmutableMap
          .builder<Any, Any>()
          .put(AcceptanceTestHarness.COLUMN_ID, 6)
          .put(AcceptanceTestHarness.COLUMN_NAME, GERALT)
          .build(),
      ),
    )
    val expectedRecords: List<JsonNode?> = preMutationRecords.toList()

    // add a new record
    source.query { ctx: DSLContext -> ctx.execute("INSERT INTO id_and_name(id, name) VALUES(6, 'geralt')") }
    // mutate a record that was already synced with out updating its cursor value. if we are actually
    // full refreshing, this record will appear in the output and cause the test to fail. if we are,
    // correctly, doing incremental, we will not find this value in the destination.
    source.query { ctx: DSLContext -> ctx.execute("UPDATE id_and_name SET name='yennefer' WHERE id=2") }

    LOGGER.info("Starting testIncrementalSync() sync 2")
    val connectionSyncRead2 = testHarness.syncConnection(connectionId)
    testHarness.waitForSuccessfulJob(connectionSyncRead2.job)

    LOGGER.info(STATE_AFTER_SYNC_TWO, testHarness.getConnectionState(connectionId))

    assertRawDestinationContains(dst, expectedRecords, conn.namespaceFormat!!, AcceptanceTestHarness.STREAM_NAME)
    assertStreamStatuses(
      testHarness,
      workspaceId,
      connectionId,
      connectionSyncRead2.job.id,
      StreamStatusRunState.COMPLETE,
      StreamStatusJobType.SYNC,
    )

    // reset back to no data.
    LOGGER.info("Starting testIncrementalSync() reset")
    val jobInfoRead = testHarness.resetConnection(connectionId)
    testHarness.waitWhileJobHasStatus(
      jobInfoRead.job,
      Sets.newHashSet(JobStatus.PENDING, JobStatus.RUNNING, JobStatus.INCOMPLETE, JobStatus.FAILED),
    )
    // This is a band-aid to prevent some race conditions where the job status was updated but we may
    // still be cleaning up some data in the reset table. This would be an argument for reworking the
    // source of truth of the replication workflow state to be in DB rather than in Memory and
    // serialized automagically by temporal
    testHarness.waitWhileJobIsRunning(jobInfoRead.job, Duration.ofMinutes(1))

    LOGGER.info("state after reset: {}", testHarness.getConnectionState(connectionId))
    assertDestinationDbEmpty(testHarness.getDestinationDatabase())

    // TODO enable once stream status for resets has been fixed
    // testHarness.assertStreamStatuses(workspaceId, connectionId, StreamStatusRunState.COMPLETE,
    // StreamStatusJobType.RESET);

    // NOTE: this is a weird usage of retry policy, but we've seen flakes where the destination still
    // has records even though the reset job is successful.
    Failsafe
      .with(
        RetryPolicy
          .builder<Any>()
          .withBackoff(Duration.ofSeconds(10), Duration.ofSeconds(600))
          .withMaxRetries(4)
          .build(),
      ).run(
        CheckedRunnable {
          assertRawDestinationContains(
            dst,
            emptyList<JsonNode>(),
            conn.namespaceFormat!!,
            AcceptanceTestHarness.STREAM_NAME,
          )
        },
      )

    // sync one more time. verify it is the equivalent of a full refresh.
    LOGGER.info("Starting testIncrementalSync() sync 3")
    val connectionSyncRead3 = testHarness.syncConnection(connectionId)
    testHarness.waitForSuccessfulJob(connectionSyncRead3.job)

    LOGGER.info("state after sync 3: {}", testHarness.getConnectionState(connectionId))

    assertSourceAndDestinationDbRawRecordsInSync(
      source = src,
      destination = dst,
      inputSchema = AcceptanceTestHarness.PUBLIC_SCHEMA_NAME,
      outputSchema = conn.namespaceFormat!!,
      withNormalizedTable = false,
      withScdTable = WITHOUT_SCD_TABLE,
    )
    assertStreamStatuses(
      testHarness,
      workspaceId,
      connectionId,
      connectionSyncRead3.job.id,
      StreamStatusRunState.COMPLETE,
      StreamStatusJobType.SYNC,
    )
  }

  @JvmRecord
  data class SyncIds(
    @JvmField val connectionId: UUID,
    @JvmField val jobId: Long,
    @JvmField val attemptNumber: Int,
  )

  @Throws(Exception::class)
  fun runSmallSyncForAWorkspaceId(workspaceId: UUID): SyncIds {
    LOGGER.info("Starting runSmallSyncForAWorkspaceId($workspaceId)")
    val sourceId = testHarness.createPostgresSource(workspaceId).sourceId
    val destinationId = testHarness.createPostgresDestination(workspaceId).destinationId
    val discoverResult = testHarness.discoverSourceSchemaWithId(sourceId)
    val retrievedCatalog = discoverResult.catalog
    val stream = retrievedCatalog!!.streams[0].stream

    Assertions.assertEquals(Lists.newArrayList(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL), stream!!.supportedSyncModes)
    Assertions.assertFalse(stream.sourceDefinedCursor!!)
    Assertions.assertTrue(stream.defaultCursorField!!.isEmpty())
    Assertions.assertTrue(stream.sourceDefinedPrimaryKey!!.isEmpty())

    val srcSyncMode = SyncMode.INCREMENTAL
    val dstSyncMode = DestinationSyncMode.APPEND
    val catalog =
      modifyCatalog(
        retrievedCatalog,
        Optional.of(srcSyncMode),
        Optional.of(dstSyncMode),
        Optional.of(listOf(AcceptanceTestHarness.COLUMN_ID)),
        Optional.empty(),
        Optional.of(true),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
      )
    val conn =
      testHarness.createConnection(
        TestConnectionCreate
          .Builder(
            sourceId,
            destinationId,
            catalog,
            discoverResult.catalogId!!,
            testHarness.dataplaneGroupId,
          ).build(),
      )
    LOGGER.info("Beginning runSmallSyncForAWorkspaceId() sync")

    val connectionId = conn.connectionId
    val connectionSyncRead1 = testHarness.syncConnection(connectionId)
    testHarness.waitForSuccessfulJob(connectionSyncRead1.job)

    LOGGER.info(STATE_AFTER_SYNC_ONE, testHarness.getConnectionState(connectionId))

    val src = testHarness.getSourceDatabase()
    val dst = testHarness.getDestinationDatabase()
    assertSourceAndDestinationDbRawRecordsInSync(
      src,
      dst,
      conn.namespaceFormat!!,
      AcceptanceTestHarness.PUBLIC_SCHEMA_NAME,
      false,
      WITHOUT_SCD_TABLE,
    )
    assertStreamStatuses(
      testHarness,
      workspaceId,
      connectionId,
      connectionSyncRead1.job.id,
      StreamStatusRunState.COMPLETE,
      StreamStatusJobType.SYNC,
    )

    // Assert that job logs exist
    val jobId = connectionSyncRead1.job.id
    val attemptId = connectionSyncRead1.attempts.size - 1
    testHarness.validateLogs(jobId, attemptId)

    return SyncIds(connectionId, jobId, attemptId)
  }

  @Throws(URISyntaxException::class, IOException::class, InterruptedException::class, GeneralSecurityException::class)
  fun init() {
    val airbyteApiClient = createAirbyteAdminApiClient()
    val testFlagsSetter = TestFlagsSetter(AIRBYTE_SERVER_HOST)

    // If a workspace id is passed, use that. Otherwise, create a new workspace.
    // NOTE: we want to sometimes use a pre-configured workspace e.g., if we run against a production
    // deployment where we don't want to create workspaces.
    // NOTE: the API client can't create workspaces in GKE deployments, so we need to provide a
    // workspace ID in that environment.
    workspaceId =
      if (System.getenv(AIRBYTE_ACCEPTANCE_TEST_WORKSPACE_ID) == null) {
        airbyteApiClient.workspaceApi
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
      } else {
        UUID.fromString(System.getenv(AIRBYTE_ACCEPTANCE_TEST_WORKSPACE_ID))
      }
    LOGGER.info("workspaceId = {}", workspaceId)

    // log which connectors are being used.
    val sourceDef =
      airbyteApiClient.sourceDefinitionApi.getSourceDefinition(
        SourceDefinitionIdRequestBody(
          POSTGRES_SOURCE_DEF_ID,
        ),
      )
    val destinationDef =
      airbyteApiClient.destinationDefinitionApi.getDestinationDefinition(
        DestinationDefinitionIdRequestBody(
          POSTGRES_DEST_DEF_ID,
        ),
      )
    LOGGER.info("pg source definition: {}", sourceDef.dockerImageTag)
    LOGGER.info("pg destination definition: {}", destinationDef.dockerImageTag)

    testHarness = AcceptanceTestHarness(apiClient = airbyteApiClient, defaultWorkspaceId = workspaceId, testFlagsSetter = testFlagsSetter)

    testHarness.ensureCleanSlate()
  }

  fun end() {
    LOGGER.debug("Executing test suite teardown")
    testHarness.stopDbAndContainers()
  }

  @Throws(SQLException::class, URISyntaxException::class, IOException::class)
  fun setup() {
    LOGGER.debug("Executing test case setup")
    testHarness.setup()
  }

  fun tearDown() {
    LOGGER.debug("Executing test case teardown")
    testHarness.cleanup()
  }

  companion object {
    private val LOGGER: Logger = LoggerFactory.getLogger(AcceptanceTestsResources::class.java)

    const val WITH_SCD_TABLE: Boolean = true
    const val WITHOUT_SCD_TABLE: Boolean = false
    const val GATEWAY_AUTH_HEADER: String = "X-Endpoint-API-UserInfo"

    // NOTE: this is just a base64 encoding of a jwt representing a test user in some deployments.
    const val CLOUD_API_USER_HEADER_VALUE: String = "eyJ1c2VyX2lkIjogImNsb3VkLWFwaSIsICJlbWFpbF92ZXJpZmllZCI6ICJ0cnVlIn0K"
    const val AIRBYTE_ACCEPTANCE_TEST_WORKSPACE_ID: String = "AIRBYTE_ACCEPTANCE_TEST_WORKSPACE_ID"
    val AIRBYTE_SERVER_HOST: String = Optional.ofNullable(System.getenv("AIRBYTE_SERVER_HOST")).orElse("http://localhost:8001")
    val POSTGRES_SOURCE_DEF_ID: UUID = UUID.fromString("decd338e-5647-4c0b-adf4-da0e75f5a750")
    val POSTGRES_DEST_DEF_ID: UUID = UUID.fromString("25c5221d-dce2-4163-ade9-739ef790f503")
    const val KUBE: String = "KUBE"
    const val TRUE: String = "true"
    const val DISABLE_TEMPORAL_TESTS_IN_GKE: String =
      "Test disabled because it specifically interacts with Temporal, which is deployment-dependent "
    const val JITTER_MAX_INTERVAL_SECS: Int = 10
    const val FINAL_INTERVAL_SECS: Int = 60
    const val MAX_TRIES: Int = 3
    const val STATE_AFTER_SYNC_ONE: String = "state after sync 1: {}"
    const val STATE_AFTER_SYNC_TWO: String = "state after sync 2: {}"
    const val GERALT: String = "geralt"
    const val MAX_SCHEDULED_JOB_RETRIES: Int = 10
  }
}
