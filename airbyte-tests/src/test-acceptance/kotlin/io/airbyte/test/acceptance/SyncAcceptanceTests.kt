/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.acceptance

import com.google.common.collect.ImmutableMap
import dev.failsafe.Failsafe
import dev.failsafe.RetryPolicy
import dev.failsafe.function.CheckedSupplier
import io.airbyte.api.client.model.generated.CheckConnectionRead
import io.airbyte.api.client.model.generated.ConnectionScheduleData
import io.airbyte.api.client.model.generated.ConnectionScheduleDataCron
import io.airbyte.api.client.model.generated.ConnectionScheduleType
import io.airbyte.api.client.model.generated.DestinationSyncMode
import io.airbyte.api.client.model.generated.JobStatus
import io.airbyte.api.client.model.generated.StreamStatusJobType
import io.airbyte.api.client.model.generated.StreamStatusRunState
import io.airbyte.api.client.model.generated.SyncMode
import io.airbyte.commons.json.Jsons
import io.airbyte.db.Database
import io.airbyte.featureflag.UseSyncV2
import io.airbyte.featureflag.Workspace
import io.airbyte.test.utils.AcceptanceTestHarness
import io.airbyte.test.utils.AcceptanceTestUtils.IS_GKE
import io.airbyte.test.utils.AcceptanceTestUtils.modifyCatalog
import io.airbyte.test.utils.Asserts.assertSourceAndDestinationDbRawRecordsInSync
import io.airbyte.test.utils.Asserts.assertStreamStatuses
import io.airbyte.test.utils.Databases.listAllTables
import io.airbyte.test.utils.Databases.retrieveRecordsFromDatabase
import io.airbyte.test.utils.TestConnectionCreate
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException
import java.time.Duration
import java.util.Optional
import java.util.Set
import java.util.UUID

// TODO switch all the tests back to normal (i.e. non-parameterized) after the sync workflow v2 rollout

/**
 * This class tests sync functionality.
 *
 *
 * Due to the number of tests here, this set runs only on the docker deployment for speed. The tests
 * here are disabled for Kubernetes as operations take much longer due to Kubernetes pod spin up
 * times and there is little value in re-running these tests since this part of the system does not
 * vary between deployments.
 *
 *
 * We order tests such that earlier tests test more basic behavior relied upon in later tests. e.g.
 * We test that we can create a destination before we test whether we can sync data to it.
 */
@Execution(ExecutionMode.CONCURRENT)
@Tag("sync")
internal abstract class SyncAcceptanceTests(
  private val useV2: Boolean,
) {
  private lateinit var testResources: AcceptanceTestsResources

  private lateinit var testHarness: AcceptanceTestHarness
  private lateinit var workspaceId: UUID

  @BeforeEach
  @Throws(Exception::class)
  fun setup() {
    testResources = AcceptanceTestsResources()
    testResources.init()
    testHarness = testResources.testHarness
    workspaceId = testResources.workspaceId
    testResources.setup()
  }

  @AfterEach
  fun tearDown() {
    testResources.tearDown()
    testResources.end()
  }

  @Test
  @DisabledIfEnvironmentVariable(named = IS_GKE, matches = AcceptanceTestsResources.TRUE, disabledReason = DUPLICATE_TEST_IN_GKE)
  @Throws(
    IOException::class,
  )
  fun testSourceCheckConnection() {
    testHarness.withFlag(UseSyncV2, Workspace(workspaceId), value = useV2).use {
      val sourceId = testHarness.createPostgresSource().sourceId

      val checkConnectionRead = testHarness.checkSource(sourceId)

      Assertions.assertEquals(
        CheckConnectionRead.Status.SUCCEEDED,
        checkConnectionRead.status,
        checkConnectionRead.message,
      )
    }
  }

  @Test
  @Throws(Exception::class)
  fun testCancelSync() {
    testHarness.withFlag(UseSyncV2, Workspace(workspaceId), value = useV2).use {
      val sourceDefinition =
        testHarness.createE2eSourceDefinition(
          workspaceId,
        )

      val source =
        testHarness.createSource(
          E2E_TEST_SOURCE + UUID.randomUUID(),
          workspaceId,
          sourceDefinition.sourceDefinitionId,
          Jsons.jsonNode(
            ImmutableMap
              .builder<Any, Any>()
              .put(TYPE, INFINITE_FEED)
              .put(MESSAGE_INTERVAL, 1000)
              .put(MAX_RECORDS, Duration.ofMinutes(5).toSeconds())
              .build(),
          ),
        )

      val sourceId = source.sourceId
      val destinationId = testHarness.createPostgresDestination().destinationId
      val discoverResult = testHarness.discoverSourceSchemaWithId(sourceId)
      val srcSyncMode = SyncMode.FULL_REFRESH
      val dstSyncMode = DestinationSyncMode.OVERWRITE
      val catalog =
        modifyCatalog(
          discoverResult.catalog,
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
          Optional.empty(),
        )
      val connectionId =
        testHarness
          .createConnection(
            TestConnectionCreate
              .Builder(
                sourceId,
                destinationId,
                catalog,
                discoverResult.catalogId!!,
                testHarness.dataplaneGroupId,
              ).build(),
          ).connectionId
      val connectionSyncRead = testHarness.syncConnection(connectionId)

      // wait to get out of PENDING
      val jobRead = testHarness.waitWhileJobHasStatus(connectionSyncRead.job, Set.of(JobStatus.PENDING))
      Assertions.assertEquals(JobStatus.RUNNING, jobRead.status)

      val resp = testHarness.cancelSync(connectionSyncRead.job.id)
      Assertions.assertEquals(JobStatus.CANCELLED, resp.job.status)
    }
  }

  @Test
  @DisabledIfEnvironmentVariable(named = IS_GKE, matches = AcceptanceTestsResources.TRUE, disabledReason = DUPLICATE_TEST_IN_GKE)
  @Throws(
    Exception::class,
  )
  fun testScheduledSync() {
    testHarness.withFlag(UseSyncV2, Workspace(workspaceId), value = useV2).use {
      val sourceId = testHarness.createPostgresSource().sourceId
      val destinationId = testHarness.createPostgresDestination().destinationId
      val discoverResult = testHarness.discoverSourceSchemaWithId(sourceId)
      val srcSyncMode = SyncMode.FULL_REFRESH
      val dstSyncMode = DestinationSyncMode.OVERWRITE
      val catalog =
        modifyCatalog(
          discoverResult.catalog,
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
            ).setSchedule(ConnectionScheduleType.BASIC, testResources.basicScheduleData)
            .build(),
        )
      val connectionId = conn.connectionId
      val jobRead = testHarness.getMostRecentSyncForConnection(connectionId)

      testResources.waitForSuccessfulJobWithRetries(jobRead)

      assertSourceAndDestinationDbRawRecordsInSync(
        testHarness.getSourceDatabase(),
        testHarness.getDestinationDatabase(),
        AcceptanceTestHarness.PUBLIC_SCHEMA_NAME,
        conn.namespaceFormat!!,
        false,
        AcceptanceTestsResources.WITHOUT_SCD_TABLE,
      )
      assertStreamStatuses(
        testHarness,
        workspaceId,
        connectionId,
        jobRead.id,
        StreamStatusRunState.COMPLETE,
        StreamStatusJobType.SYNC,
      )
    }
  }

  @Test
  @Throws(Exception::class)
  fun testCronSync() {
    testHarness.withFlag(UseSyncV2, Workspace(workspaceId), value = useV2).use {
      val sourceId = testHarness.createPostgresSource().sourceId
      val destinationId = testHarness.createPostgresDestination().destinationId
      val discoverResult = testHarness.discoverSourceSchemaWithId(sourceId)

      // NOTE: this cron should run once every two minutes.
      val connectionScheduleData =
        ConnectionScheduleData(
          null,
          ConnectionScheduleDataCron("0 */2 * * * ?", "UTC"),
        )
      val srcSyncMode = SyncMode.FULL_REFRESH
      val dstSyncMode = DestinationSyncMode.OVERWRITE
      val catalog =
        modifyCatalog(
          discoverResult.catalog,
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
            ).setSchedule(ConnectionScheduleType.CRON, connectionScheduleData)
            .build(),
        )

      val connectionId = conn.connectionId
      val jobRead = testHarness.getMostRecentSyncForConnection(connectionId)

      testResources.waitForSuccessfulJobWithRetries(jobRead)

      // NOTE: this is an unusual use of a retry policy. Sometimes the raw tables haven't been cleaned up
      // even though the job
      // is marked successful.
      val retryAssertOutcome =
        Failsafe
          .with<Any, RetryPolicy<Any>>(
            RetryPolicy
              .builder<Any>()
              .withBackoff(
                Duration.ofSeconds(AcceptanceTestsResources.JITTER_MAX_INTERVAL_SECS.toLong()),
                Duration.ofSeconds(
                  AcceptanceTestsResources.FINAL_INTERVAL_SECS.toLong(),
                ),
              ).withMaxRetries(AcceptanceTestsResources.MAX_TRIES)
              .build(),
          ).get<String>(
            CheckedSupplier<String> {
              assertSourceAndDestinationDbRawRecordsInSync(
                testHarness.getSourceDatabase(),
                testHarness.getDestinationDatabase(),
                AcceptanceTestHarness.PUBLIC_SCHEMA_NAME,
                conn.namespaceFormat!!,
                false,
                AcceptanceTestsResources.WITHOUT_SCD_TABLE,
              )
              "success" // If the assertion throws after all the retries, then retryWithJitter will return null.
            },
          )
      Assertions.assertEquals("success", retryAssertOutcome)

      assertStreamStatuses(
        testHarness,
        workspaceId,
        connectionId,
        jobRead.id,
        StreamStatusRunState.COMPLETE,
        StreamStatusJobType.SYNC,
      )

      testHarness.deleteConnection(connectionId)

      // remove connection to avoid exception during tear down
      testHarness.removeConnection(connectionId)
    }
  }

  @Test
  fun testIncrementalSync() {
    testHarness.withFlag(UseSyncV2, Workspace(workspaceId), value = useV2).use {
      testResources.runIncrementalSyncForAWorkspaceId(workspaceId)
    }
  }

  @Test
  @DisabledIfEnvironmentVariable(
    named = IS_GKE,
    matches = AcceptanceTestsResources.TRUE,
    disabledReason = "The different way of interacting with the source db causes errors",
  )
  @Throws(
    Exception::class,
  )
  fun testMultipleSchemasAndTablesSyncAndReset() {
    testHarness.withFlag(UseSyncV2, Workspace(workspaceId), value = useV2).use {
      // create tables in another schema
      // NOTE: this command fails in GKE because we already ran it in a previous test case and we use the
      // same
      // database instance across the test suite. To get it to work, we need to do something better with
      // cleanup.
      testHarness.runSqlScriptInSource("postgres_second_schema_multiple_tables.sql")

      val sourceId = testHarness.createPostgresSource().sourceId
      val destinationId = testHarness.createPostgresDestination().destinationId
      val discoverResult = testHarness.discoverSourceSchemaWithId(sourceId)
      val srcSyncMode = SyncMode.FULL_REFRESH
      val dstSyncMode = DestinationSyncMode.OVERWRITE
      val catalog =
        modifyCatalog(
          discoverResult.catalog,
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
          Optional.empty(),
        )
      val conn =
        testHarness.createConnectionSourceNamespace(
          TestConnectionCreate
            .Builder(
              sourceId,
              destinationId,
              catalog,
              discoverResult.catalogId!!,
              testHarness.dataplaneGroupId,
            ).build(),
        )

      val connectionId = conn.connectionId
      val connectionSyncRead = testHarness.syncConnection(connectionId)
      testHarness.waitForSuccessfulJob(connectionSyncRead.job)

      assertSourceAndDestinationDbRawRecordsInSync(
        testHarness.getSourceDatabase(),
        testHarness.getDestinationDatabase(),
        AcceptanceTestHarness.PUBLIC_SCHEMA_NAME,
        conn.namespaceFormat!!.replace("\${SOURCE_NAMESPACE}", AcceptanceTestHarness.PUBLIC),
        false,
        AcceptanceTestsResources.WITHOUT_SCD_TABLE,
      )
      assertSourceAndDestinationDbRawRecordsInSync(
        testHarness.getSourceDatabase(),
        testHarness.getDestinationDatabase(),
        "staging",
        conn.namespaceFormat!!.replace("\${SOURCE_NAMESPACE}", "staging"),
        false,
        false,
      )
      val connectionResetRead = testHarness.resetConnection(connectionId)
      testHarness.waitForSuccessfulJob(connectionResetRead.job)
      assertDestinationDbEmpty(testHarness.getDestinationDatabase())
    }
  }

  companion object {
    private val LOGGER: Logger = LoggerFactory.getLogger(SyncAcceptanceTests::class.java)

    const val SLOW_TEST_IN_GKE: String = "TODO(https://github.com/airbytehq/airbyte-platform-internal/issues/5181): re-enable slow tests in GKE"
    const val DUPLICATE_TEST_IN_GKE: String =
      "TODO(https://github.com/airbytehq/airbyte-platform-internal/issues/5182): eliminate test duplication"
    const val TYPE: String = "type"
    const val E2E_TEST_SOURCE: String = "E2E Test Source -"
    const val INFINITE_FEED: String = "INFINITE_FEED"
    const val MESSAGE_INTERVAL: String = "message_interval"
    const val MAX_RECORDS: String = "max_records"
    const val FIELD: String = "field"
    const val ID_AND_NAME: String = "id_and_name"

    @Throws(Exception::class)
    fun assertDestinationDbEmpty(dst: Database) {
      val destinationTables = listAllTables(dst)

      for (pair in destinationTables) {
        val recs = retrieveRecordsFromDatabase(dst, pair.getFullyQualifiedTableName())
        Assertions.assertTrue(recs.isEmpty())
      }
    }
  }
}

internal class SyncAcceptanceTestsLegacy : SyncAcceptanceTests(useV2 = false)

internal class SyncAcceptanceTestsV2 : SyncAcceptanceTests(useV2 = true)
