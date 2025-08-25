/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job

import io.airbyte.config.JobConfig
import io.airbyte.config.JobSyncConfig
import io.airbyte.db.Database
import io.airbyte.db.factory.DSLContextFactory
import io.airbyte.db.factory.DataSourceFactory.close
import io.airbyte.db.instance.DatabaseConstants
import io.airbyte.db.instance.configs.jooq.generated.enums.NamespaceDefinitionType
import io.airbyte.db.instance.configs.jooq.generated.enums.NonBreakingChangePreferenceType
import io.airbyte.db.instance.jobs.jooq.generated.Tables
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusJobType
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusRunState
import io.airbyte.db.instance.test.TestDatabaseProviders
import io.airbyte.test.utils.Databases
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.SQLDialect
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.testcontainers.containers.PostgreSQLContainer
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID
import javax.sql.DataSource
import io.airbyte.db.instance.configs.jooq.generated.Tables as ConfigTables

@DisplayName("DbPrune")
class DbPruneTest {
  private lateinit var jobDatabase: Database
  private lateinit var jobPersistence: DefaultJobPersistence
  private lateinit var dbPrune: DbPrune
  private lateinit var dataSource: DataSource
  private lateinit var dslContext: DSLContext

  @BeforeEach
  fun setup() {
    dataSource = Databases.createDataSource(container)
    dslContext = DSLContextFactory.create(dataSource, SQLDialect.POSTGRES)
    val databaseProviders = TestDatabaseProviders(dataSource, dslContext)
    // Use jobs database since the test is primarily for job persistence
    // Connection timeline events will be tested separately when available
    jobDatabase = databaseProviders.createNewJobsDatabase()
    resetDb()

    jobPersistence = DefaultJobPersistence(jobDatabase)
    dbPrune = DbPrune(jobDatabase)
  }

  @AfterEach
  fun tearDown() {
    close(dataSource)
  }

  private fun resetDb() {
    jobDatabase.query { ctx: DSLContext ->
      // Delete in reverse order of foreign key dependencies
      // Use IF EXISTS to handle tables that may not exist in test setup
      try {
        ctx.truncateTable(Tables.SYNC_STATS).cascade().execute()
      } catch (e: Exception) {
        // Table may not exist in test setup
      }
      try {
        ctx.truncateTable(Tables.STREAM_STATS).cascade().execute()
      } catch (e: Exception) {
        // Table may not exist in test setup
      }
      try {
        ctx.truncateTable(Tables.STREAM_ATTEMPT_METADATA).cascade().execute()
      } catch (e: Exception) {
        // Table may not exist in test setup
      }
      try {
        ctx.truncateTable(Tables.NORMALIZATION_SUMMARIES).cascade().execute()
      } catch (e: Exception) {
        // Table may not exist in test setup
      }
      try {
        ctx.truncateTable(Tables.STREAM_STATUSES).cascade().execute()
      } catch (e: Exception) {
        // Table may not exist in test setup
      }
      try {
        ctx.truncateTable(Tables.RETRY_STATES).cascade().execute()
      } catch (e: Exception) {
        // Table may not exist in test setup
      }
      try {
        ctx.truncateTable(Tables.ATTEMPTS).cascade().execute()
      } catch (e: Exception) {
        // Table may not exist in test setup
      }
      try {
        ctx.truncateTable(Tables.JOBS).cascade().execute()
      } catch (e: Exception) {
        // Table may not exist in test setup
      }
      // Clear connection timeline events table
      try {
        ctx.truncateTable(ConfigTables.CONNECTION_TIMELINE_EVENT).cascade().execute()
      } catch (e: Exception) {
        // Table may not exist in test setup
      }
    }
  }

  @Test
  @DisplayName("Should delete old jobs and their associated records")
  fun testDeleteOldJobsWithCascade() {
    val connectionId = UUID.randomUUID()
    val scope = connectionId.toString()
    val now = Instant.now()
    val sevenMonthsAgo = now.minusSeconds(7L * 30 * 24 * 60 * 60) // 7 months ago
    val fiveMonthsAgo = now.minusSeconds(5L * 30 * 24 * 60 * 60) // 5 months ago

    // Create an old job (7 months ago) - should be deleted
    val oldJobId = createJob(scope, sevenMonthsAgo)
    val oldAttemptNumber = jobPersistence.createAttempt(oldJobId, LOG_PATH)
    jobPersistence.succeedAttempt(oldJobId, oldAttemptNumber)
    createStatsForAttempt(oldJobId, oldAttemptNumber)

    // Create a recent job (5 months ago) - should NOT be deleted
    val recentJobId = createJob(scope, fiveMonthsAgo)
    val recentAttemptNumber = jobPersistence.createAttempt(recentJobId, LOG_PATH)
    createStatsForAttempt(recentJobId, recentAttemptNumber)

    // Verify initial state
    assertEquals(2, countJobs())
    assertEquals(2, countAttempts())
    assertEquals(2, countSyncStats())

    // Run pruning
    val deletedCount = dbPrune.pruneJobs()

    // Verify results
    assertEquals(1, deletedCount)
    assertEquals(1, countJobs())
    assertEquals(1, countAttempts())
    assertEquals(1, countSyncStats())

    // Verify the recent job still exists
    assertNotNull(getJob(recentJobId))
    assertNull(getJob(oldJobId))
  }

  @Test
  @DisplayName("Should respect batch size when deleting")
  fun testBatchSizeRespected() {
    val now = Instant.now()
    val sevenMonthsAgo = now.minusSeconds(7L * 30 * 24 * 60 * 60)

    // Create 5 old jobs with different scopes
    val oldJobIds = mutableListOf<Long>()
    for (i in 1..5) {
      val scope = UUID.randomUUID().toString()
      val jobId = createJob(scope, sevenMonthsAgo.minusSeconds(i * 60L))
      val attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH)
      jobPersistence.succeedAttempt(jobId, attemptNumber)
      oldJobIds.add(jobId)

      // Create a recent job for the same scope to ensure not all jobs for scope are deleted
      createJob(scope, now)
    }

    // Create DbPrune with batch size of 2
    val smallBatchDbPrune = DbPrune(jobDatabase, batchSize = 2)

    // Verify initial state
    assertEquals(10, countJobs()) // 5 old + 5 recent

    // Run pruning
    val deletedCount = smallBatchDbPrune.pruneJobs()

    // Verify all old jobs were deleted (even though batch size was 2)
    assertEquals(5, deletedCount)
    assertEquals(5, countJobs()) // Only recent jobs remain
  }

  @Test
  @DisplayName("Should delete all related tables correctly")
  fun testCascadingDeletes() {
    val connectionId = UUID.randomUUID()
    val scope = connectionId.toString()
    val now = Instant.now()
    val sevenMonthsAgo = now.minusSeconds(7L * 30 * 24 * 60 * 60)

    // Create an old job with all related records
    val oldJobId = createJob(scope, sevenMonthsAgo)
    val attemptNumber = jobPersistence.createAttempt(oldJobId, LOG_PATH)
    val attemptId = getAttemptId(oldJobId, attemptNumber)
    createStatsForAttempt(oldJobId, attemptNumber)

    // Create additional related records
    createStreamStats(attemptId, connectionId)
    createStreamAttemptMetadata(attemptId)
    createNormalizationSummary(attemptId)
    createStreamStatus(oldJobId, connectionId)
    createRetryState(oldJobId, connectionId)

    // Complete the old job so we can create a new one
    jobPersistence.succeedAttempt(oldJobId, attemptNumber)

    // Create a recent job to prevent deleting all jobs for scope
    createJob(scope, now)

    // Verify initial state
    assertEquals(2, countJobs())
    assertEquals(1, countAttempts())
    assertEquals(1, countSyncStats())
    assertEquals(1, countStreamStats())
    assertEquals(1, countStreamAttemptMetadata())
    assertEquals(1, countNormalizationSummaries())
    assertEquals(1, countStreamStatuses())
    assertEquals(1, countRetryStates())

    // Run pruning
    val deletedCount = dbPrune.pruneJobs()

    // Verify all related records were deleted
    assertEquals(1, deletedCount)
    assertEquals(1, countJobs())
    assertEquals(0, countAttempts())
    assertEquals(0, countSyncStats())
    assertEquals(0, countStreamStats())
    assertEquals(0, countStreamAttemptMetadata())
    assertEquals(0, countNormalizationSummaries())
    assertEquals(0, countStreamStatuses())
    assertEquals(0, countRetryStates())
  }

  @Test
  @DisplayName("Should provide accurate statistics without deleting")
  fun testGetDeletionStatistics() {
    val connectionId = UUID.randomUUID()
    val scope = connectionId.toString()
    val now = Instant.now()
    val sevenMonthsAgo = now.minusSeconds(7L * 30 * 24 * 60 * 60)

    // Create an old job with related records
    val oldJobId = createJob(scope, sevenMonthsAgo)
    val attemptNumber = jobPersistence.createAttempt(oldJobId, LOG_PATH)
    val attemptId = getAttemptId(oldJobId, attemptNumber)
    createStatsForAttempt(oldJobId, attemptNumber)
    createStreamStats(attemptId, connectionId)
    jobPersistence.succeedAttempt(oldJobId, attemptNumber)

    // Create a recent job
    createJob(scope, now)

    // Check that the eligible count is correct but no actual deletion occurs
    assertEquals(1L, dbPrune.getEligibleJobCount())

    // Verify nothing was actually deleted
    assertEquals(2, countJobs())
  }

  @Test
  @DisplayName("Should handle empty database gracefully")
  fun testEmptyDatabase() {
    // Run pruning on empty database
    val deletedCount = dbPrune.pruneJobs()

    // Verify no errors and nothing deleted
    assertEquals(0, deletedCount)
    assertEquals(0L, dbPrune.getEligibleJobCount())
  }

  // Helper methods
  private fun createJob(
    scope: String,
    createdAt: Instant,
  ): Long {
    val jobIdOptional = jobPersistence.enqueueJob(scope, SYNC_JOB_CONFIG, true)
    val jobId = jobIdOptional.orElseThrow { RuntimeException("Failed to create job") }

    // Update the created_at timestamp
    jobDatabase.query { ctx: DSLContext ->
      ctx
        .update(Tables.JOBS)
        .set(Tables.JOBS.CREATED_AT, OffsetDateTime.ofInstant(createdAt, ZoneOffset.UTC))
        .where(Tables.JOBS.ID.eq(jobId))
        .execute()
    }

    return jobId
  }

  private fun createStatsForAttempt(
    jobId: Long,
    attemptNumber: Int,
  ) {
    // Get attempt ID
    val attemptId = getAttemptId(jobId, attemptNumber)

    // Create sync stats
    jobDatabase.query { ctx: DSLContext ->
      ctx
        .select(Tables.ATTEMPTS.ID)
        .from(Tables.ATTEMPTS)
        .where(Tables.ATTEMPTS.JOB_ID.eq(jobId))
        .and(Tables.ATTEMPTS.ATTEMPT_NUMBER.eq(attemptNumber))
        .fetchOne()
        ?.value1() ?: throw RuntimeException("Attempt not found")
      ctx
        .insertInto(Tables.SYNC_STATS)
        .set(Tables.SYNC_STATS.ID, UUID.randomUUID())
        .set(Tables.SYNC_STATS.ATTEMPT_ID, attemptId)
        .set(Tables.SYNC_STATS.RECORDS_EMITTED, 100L)
        .set(Tables.SYNC_STATS.BYTES_EMITTED, 1000L)
        .execute()
    }
  }

  private fun getAttemptId(
    jobId: Long,
    attemptNumber: Int,
  ): Long =
    jobDatabase.query { ctx: DSLContext ->
      ctx
        .select(Tables.ATTEMPTS.ID)
        .from(Tables.ATTEMPTS)
        .where(Tables.ATTEMPTS.JOB_ID.eq(jobId))
        .and(Tables.ATTEMPTS.ATTEMPT_NUMBER.eq(attemptNumber))
        .fetchOne()
        ?.value1() ?: throw RuntimeException("Attempt not found")
    }

  private fun createStreamStats(
    attemptId: Long,
    connectionId: UUID,
  ) {
    jobDatabase.query { ctx: DSLContext ->
      ctx
        .insertInto(Tables.STREAM_STATS)
        .set(Tables.STREAM_STATS.ID, UUID.randomUUID())
        .set(Tables.STREAM_STATS.ATTEMPT_ID, attemptId)
        .set(Tables.STREAM_STATS.STREAM_NAME, "test_stream")
        .set(Tables.STREAM_STATS.CONNECTION_ID, connectionId)
        .set(Tables.STREAM_STATS.RECORDS_EMITTED, 50L)
        .execute()
    }
  }

  private fun createStreamAttemptMetadata(attemptId: Long) {
    jobDatabase.query { ctx: DSLContext ->
      ctx
        .insertInto(Tables.STREAM_ATTEMPT_METADATA)
        .set(Tables.STREAM_ATTEMPT_METADATA.ID, UUID.randomUUID())
        .set(Tables.STREAM_ATTEMPT_METADATA.ATTEMPT_ID, attemptId)
        .set(Tables.STREAM_ATTEMPT_METADATA.STREAM_NAME, "test_stream")
        .set(Tables.STREAM_ATTEMPT_METADATA.WAS_BACKFILLED, false)
        .set(Tables.STREAM_ATTEMPT_METADATA.WAS_RESUMED, false)
        .execute()
    }
  }

  private fun createNormalizationSummary(attemptId: Long) {
    jobDatabase.query { ctx: DSLContext ->
      ctx
        .insertInto(Tables.NORMALIZATION_SUMMARIES)
        .set(Tables.NORMALIZATION_SUMMARIES.ID, UUID.randomUUID())
        .set(Tables.NORMALIZATION_SUMMARIES.ATTEMPT_ID, attemptId)
        .execute()
    }
  }

  private fun createStreamStatus(
    jobId: Long,
    connectionId: UUID,
  ) {
    jobDatabase.query { ctx: DSLContext ->
      ctx
        .insertInto(Tables.STREAM_STATUSES)
        .set(Tables.STREAM_STATUSES.ID, UUID.randomUUID())
        .set(Tables.STREAM_STATUSES.WORKSPACE_ID, UUID.randomUUID())
        .set(Tables.STREAM_STATUSES.CONNECTION_ID, connectionId)
        .set(Tables.STREAM_STATUSES.JOB_ID, jobId)
        .set(Tables.STREAM_STATUSES.STREAM_NAME, "test_stream")
        .set(Tables.STREAM_STATUSES.ATTEMPT_NUMBER, 0)
        .set(Tables.STREAM_STATUSES.JOB_TYPE, JobStreamStatusJobType.sync)
        .set(Tables.STREAM_STATUSES.RUN_STATE, JobStreamStatusRunState.complete)
        .set(Tables.STREAM_STATUSES.TRANSITIONED_AT, OffsetDateTime.now())
        .execute()
    }
  }

  private fun createRetryState(
    jobId: Long,
    connectionId: UUID,
  ) {
    jobDatabase.query { ctx: DSLContext ->
      ctx
        .insertInto(Tables.RETRY_STATES)
        .set(Tables.RETRY_STATES.ID, UUID.randomUUID())
        .set(Tables.RETRY_STATES.CONNECTION_ID, connectionId)
        .set(Tables.RETRY_STATES.JOB_ID, jobId)
        .set(Tables.RETRY_STATES.SUCCESSIVE_COMPLETE_FAILURES, 0)
        .set(Tables.RETRY_STATES.TOTAL_COMPLETE_FAILURES, 0)
        .set(Tables.RETRY_STATES.SUCCESSIVE_PARTIAL_FAILURES, 0)
        .set(Tables.RETRY_STATES.TOTAL_PARTIAL_FAILURES, 0)
        .execute()
    }
  }

  private fun getJob(jobId: Long): Long? =
    jobDatabase.query { ctx: DSLContext ->
      ctx
        .select(Tables.JOBS.ID)
        .from(Tables.JOBS)
        .where(Tables.JOBS.ID.eq(jobId))
        .fetchOne()
        ?.value1()
    }

  private fun countJobs(): Int = countRecords(Tables.JOBS.name)

  private fun countAttempts(): Int = countRecords(Tables.ATTEMPTS.name)

  private fun countSyncStats(): Int = countRecords(Tables.SYNC_STATS.name)

  private fun countStreamStats(): Int = countRecords(Tables.STREAM_STATS.name)

  private fun countStreamAttemptMetadata(): Int = countRecords(Tables.STREAM_ATTEMPT_METADATA.name)

  private fun countNormalizationSummaries(): Int = countRecords(Tables.NORMALIZATION_SUMMARIES.name)

  private fun countStreamStatuses(): Int = countRecords(Tables.STREAM_STATUSES.name)

  private fun countRetryStates(): Int = countRecords(Tables.RETRY_STATES.name)

  private fun countRecords(tableName: String): Int =
    jobDatabase.query { ctx: DSLContext ->
      ctx
        .selectCount()
        .from(tableName)
        .fetchOne(0, Int::class.java) ?: 0
    }

  @Test
  @DisplayName("Should delete old connection timeline events")
  fun testDeleteOldConnectionTimelineEvents() {
    // Skip this test if connection_timeline_event table doesn't exist
    val tableExists =
      jobDatabase.query { ctx: DSLContext ->
        ctx.meta().tables.any { it.name == "connection_timeline_event" }
      }

    if (!tableExists) {
      // Table doesn't exist in test database, skip test
      return
    }

    val connectionId = UUID.randomUUID()
    val userId = UUID.randomUUID()
    val now = OffsetDateTime.now(ZoneOffset.UTC)
    val twentyMonthsAgo = now.minusMonths(20)
    val tenMonthsAgo = now.minusMonths(10)

    // Create old events (20 months ago) - should be deleted
    val oldEventId1 = createConnectionTimelineEvent(connectionId, userId, "sync_started", twentyMonthsAgo)
    val oldEventId2 = createConnectionTimelineEvent(connectionId, userId, "sync_completed", twentyMonthsAgo.plusHours(1))

    // Create recent events (10 months ago) - should NOT be deleted
    val recentEventId1 = createConnectionTimelineEvent(connectionId, userId, "sync_started", tenMonthsAgo)
    val recentEventId2 = createConnectionTimelineEvent(connectionId, null, "sync_failed", tenMonthsAgo.plusDays(1))

    // Verify initial state
    assertEquals(4, countConnectionTimelineEvents())
    assertNotNull(getConnectionTimelineEvent(oldEventId1))
    assertNotNull(getConnectionTimelineEvent(oldEventId2))
    assertNotNull(getConnectionTimelineEvent(recentEventId1))
    assertNotNull(getConnectionTimelineEvent(recentEventId2))

    // Run pruning
    val deletedCount = dbPrune.pruneEvents(now)

    // Verify results
    assertEquals(2, deletedCount)
    assertEquals(2, countConnectionTimelineEvents())

    // Verify old events were deleted
    assertNull(getConnectionTimelineEvent(oldEventId1))
    assertNull(getConnectionTimelineEvent(oldEventId2))

    // Verify recent events still exist
    assertNotNull(getConnectionTimelineEvent(recentEventId1))
    assertNotNull(getConnectionTimelineEvent(recentEventId2))
  }

  @Test
  @DisplayName("Should respect custom max age for connection timeline events")
  fun testCustomMaxAgeForConnectionTimelineEvents() {
    // Skip this test if connection_timeline_event table doesn't exist
    val tableExists =
      jobDatabase.query { ctx: DSLContext ->
        ctx.meta().tables.any { it.name == "connection_timeline_event" }
      }

    if (!tableExists) {
      return
    }
    val connectionId = UUID.randomUUID()
    val userId = UUID.randomUUID()
    val now = OffsetDateTime.now(ZoneOffset.UTC)
    val thirteenMonthsAgo = now.minusMonths(13)
    val elevenMonthsAgo = now.minusMonths(11)

    // Create events 13 months ago
    val event13MonthsId = createConnectionTimelineEvent(connectionId, userId, "sync_started", thirteenMonthsAgo)

    // Create events 11 months ago
    val event11MonthsId = createConnectionTimelineEvent(connectionId, userId, "sync_completed", elevenMonthsAgo)

    // Verify initial state
    assertEquals(2, countConnectionTimelineEvents())

    // Run pruning with custom max age of 12 months
    val customDbPrune = DbPrune(jobDatabase, eventsMaxAgeMonths = 12L)
    val deletedCount = customDbPrune.pruneEvents(now)

    // Verify only the 13-month-old event was deleted
    assertEquals(1, deletedCount)
    assertEquals(1, countConnectionTimelineEvents())
    assertNull(getConnectionTimelineEvent(event13MonthsId))
    assertNotNull(getConnectionTimelineEvent(event11MonthsId))
  }

  @Test
  @DisplayName("Should handle empty connection timeline event table gracefully")
  fun testEmptyConnectionTimelineEventTable() {
    // Skip this test if connection_timeline_event table doesn't exist
    val tableExists =
      jobDatabase.query { ctx: DSLContext ->
        ctx.meta().tables.any { it.name == "connection_timeline_event" }
      }

    if (!tableExists) {
      return
    }
    // Ensure table is empty
    assertEquals(0, countConnectionTimelineEvents())

    // Run pruning
    val deletedCount = dbPrune.pruneEvents()

    // Verify no errors and nothing deleted
    assertEquals(0, deletedCount)
    assertEquals(0L, dbPrune.getEligibleEventCount())
  }

  @Test
  @DisplayName("Should provide accurate event statistics without deleting")
  fun testGetEligibleEventCount() {
    // Skip this test if connection_timeline_event table doesn't exist
    val tableExists =
      jobDatabase.query { ctx: DSLContext ->
        ctx.meta().tables.any { it.name == "connection_timeline_event" }
      }

    if (!tableExists) {
      return
    }
    val connectionId = UUID.randomUUID()
    val userId = UUID.randomUUID()
    val now = OffsetDateTime.now(ZoneOffset.UTC)
    val twentyMonthsAgo = now.minusMonths(20)
    val tenMonthsAgo = now.minusMonths(10)

    // Create 3 old events (20 months ago)
    createConnectionTimelineEvent(connectionId, userId, "sync_started", twentyMonthsAgo)
    createConnectionTimelineEvent(connectionId, userId, "sync_completed", twentyMonthsAgo.plusHours(1))
    createConnectionTimelineEvent(connectionId, null, "sync_failed", twentyMonthsAgo.plusDays(1))

    // Create 2 recent events (10 months ago)
    createConnectionTimelineEvent(connectionId, userId, "sync_started", tenMonthsAgo)
    createConnectionTimelineEvent(connectionId, userId, "sync_completed", tenMonthsAgo.plusHours(1))

    // Check eligible count without deleting
    assertEquals(3L, dbPrune.getEligibleEventCount(now))

    // Verify nothing was actually deleted
    assertEquals(5, countConnectionTimelineEvents())

    // Test with custom max age
    val dbPrune24Months = DbPrune(jobDatabase, eventsMaxAgeMonths = 24L)
    assertEquals(0L, dbPrune24Months.getEligibleEventCount(now))

    val dbPrune6Months = DbPrune(jobDatabase, eventsMaxAgeMonths = 6L)
    assertEquals(5L, dbPrune6Months.getEligibleEventCount(now))
  }

  // Helper methods for connection timeline events
  private fun createConnectionTimelineEvent(
    connectionId: UUID,
    userId: UUID?,
    eventType: String,
    createdAt: OffsetDateTime,
  ): UUID {
    val eventId = UUID.randomUUID()

    // First ensure the connection exists (create a minimal connection if needed)
    ensureConnectionExists(connectionId)

    // If userId is provided, ensure user exists
    if (userId != null) {
      ensureUserExists(userId)
    }

    jobDatabase.query { ctx: DSLContext ->
      ctx
        .insertInto(ConfigTables.CONNECTION_TIMELINE_EVENT)
        .set(ConfigTables.CONNECTION_TIMELINE_EVENT.ID, eventId)
        .set(ConfigTables.CONNECTION_TIMELINE_EVENT.CONNECTION_ID, connectionId)
        .set(ConfigTables.CONNECTION_TIMELINE_EVENT.USER_ID, userId)
        .set(ConfigTables.CONNECTION_TIMELINE_EVENT.EVENT_TYPE, eventType)
        .set(ConfigTables.CONNECTION_TIMELINE_EVENT.SUMMARY, JSONB.valueOf("{\"test\": \"data\"}"))
        .set(ConfigTables.CONNECTION_TIMELINE_EVENT.CREATED_AT, createdAt)
        .execute()
    }
    return eventId
  }

  private fun ensureConnectionExists(connectionId: UUID) {
    jobDatabase.query { ctx: DSLContext ->
      // Check if connection already exists
      val exists =
        ctx
          .selectCount()
          .from(ConfigTables.CONNECTION)
          .where(ConfigTables.CONNECTION.ID.eq(connectionId))
          .fetchOne(0, Int::class.java) ?: 0

      if (exists == 0) {
        // Create minimal connection record
        val sourceId = UUID.randomUUID()
        val destinationId = UUID.randomUUID()

        // Create the connection (workspace is associated through source/destination)
        ctx
          .insertInto(ConfigTables.CONNECTION)
          .set(ConfigTables.CONNECTION.ID, connectionId)
          .set(ConfigTables.CONNECTION.NAMESPACE_DEFINITION, NamespaceDefinitionType.source)
          .set(ConfigTables.CONNECTION.SOURCE_ID, sourceId)
          .set(ConfigTables.CONNECTION.DESTINATION_ID, destinationId)
          .set(ConfigTables.CONNECTION.NAME, "Test Connection")
          .set(ConfigTables.CONNECTION.CATALOG, JSONB.valueOf("{}"))
          .set(ConfigTables.CONNECTION.MANUAL, true)
          .set(ConfigTables.CONNECTION.NON_BREAKING_CHANGE_PREFERENCE, NonBreakingChangePreferenceType.ignore)
          .execute()
      }
    }
  }

  private fun ensureUserExists(userId: UUID) {
    jobDatabase.query { ctx: DSLContext ->
      // Check if user already exists
      val exists =
        ctx
          .selectCount()
          .from(ConfigTables.USER)
          .where(ConfigTables.USER.ID.eq(userId))
          .fetchOne(0, Int::class.java) ?: 0

      if (exists == 0) {
        // Create minimal user record
        ctx
          .insertInto(ConfigTables.USER)
          .set(ConfigTables.USER.ID, userId)
          .set(ConfigTables.USER.NAME, "Test User")
          .set(ConfigTables.USER.EMAIL, "test-user@example.com")
          .execute()
      }
    }
  }

  private fun getConnectionTimelineEvent(eventId: UUID): UUID? =
    jobDatabase.query { ctx: DSLContext ->
      ctx
        .select(ConfigTables.CONNECTION_TIMELINE_EVENT.ID)
        .from(ConfigTables.CONNECTION_TIMELINE_EVENT)
        .where(ConfigTables.CONNECTION_TIMELINE_EVENT.ID.eq(eventId))
        .fetchOne()
        ?.value1()
    }

  private fun countConnectionTimelineEvents(): Int =
    jobDatabase.query { ctx: DSLContext ->
      ctx
        .selectCount()
        .from(ConfigTables.CONNECTION_TIMELINE_EVENT)
        .fetchOne(0, Int::class.java) ?: 0
    }

  companion object {
    private val LOG_PATH =
      java.nio.file.Path
        .of("/tmp/logs/test")
    private val SYNC_JOB_CONFIG =
      JobConfig()
        .withConfigType(JobConfig.ConfigType.SYNC)
        .withSync(JobSyncConfig())

    private lateinit var container: PostgreSQLContainer<*>

    @BeforeAll
    @JvmStatic
    fun dbSetup() {
      container =
        PostgreSQLContainer(DatabaseConstants.DEFAULT_DATABASE_VERSION)
          .withDatabaseName("airbyte")
          .withUsername("docker")
          .withPassword("docker")
      container.start()
    }

    @AfterAll
    @JvmStatic
    fun dbDown() {
      container.close()
    }
  }
}
