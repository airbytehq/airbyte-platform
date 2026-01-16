/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job

import io.airbyte.commons.json.Jsons.canonicalJsonSerialize
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.commons.version.AirbyteProtocolVersion
import io.airbyte.commons.version.AirbyteProtocolVersionRange
import io.airbyte.commons.version.Version
import io.airbyte.config.Attempt
import io.airbyte.config.AttemptFailureSummary
import io.airbyte.config.AttemptStatus
import io.airbyte.config.AttemptSyncConfig
import io.airbyte.config.FailureReason
import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.JobConfig.ConfigType
import io.airbyte.config.JobGetSpecConfig
import io.airbyte.config.JobOutput
import io.airbyte.config.JobStatus
import io.airbyte.config.JobStatusSummary
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.StandardSyncOutput
import io.airbyte.config.StandardSyncSummary
import io.airbyte.config.State
import io.airbyte.config.StreamSyncStats
import io.airbyte.config.SyncStats
import io.airbyte.db.ContextQueryFunction
import io.airbyte.db.Database
import io.airbyte.db.factory.DSLContextFactory
import io.airbyte.db.factory.DataSourceFactory.close
import io.airbyte.db.instance.DatabaseConstants
import io.airbyte.db.instance.jobs.jooq.generated.Tables
import io.airbyte.db.instance.jobs.jooq.generated.tables.records.StreamAttemptMetadataRecord
import io.airbyte.db.instance.jobs.jooq.generated.tables.records.SyncStatsRecord
import io.airbyte.db.instance.test.TestDatabaseProviders
import io.airbyte.persistence.job.JobPersistence.JobAttemptPair
import io.airbyte.test.utils.Databases
import io.mockk.every
import io.mockk.mockk
import org.jooq.DSLContext
import org.jooq.Record
import org.jooq.Result
import org.jooq.SQLDialect
import org.jooq.TruncateIdentityStep
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.testcontainers.containers.PostgreSQLContainer
import java.nio.file.Path
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.Optional
import java.util.UUID
import java.util.function.Supplier
import java.util.stream.Collectors
import javax.sql.DataSource

@DisplayName("DefaultJobPersistence")
internal class DefaultJobPersistenceTest {
  private lateinit var jobDatabase: Database
  private lateinit var timeSupplier: Supplier<Instant>
  private lateinit var jobPersistence: DefaultJobPersistence
  private lateinit var dataSource: DataSource
  private lateinit var dslContext: DSLContext

  @BeforeEach
  fun setup() {
    dataSource = Databases.createDataSource(container)
    dslContext = DSLContextFactory.create(dataSource, SQLDialect.POSTGRES)
    val databaseProviders = TestDatabaseProviders(dataSource, dslContext)
    jobDatabase = databaseProviders.createNewJobsDatabase()
    resetDb()

    timeSupplier = mockk<Supplier<Instant>>()
    every { timeSupplier.get() } returns NOW

    jobPersistence = DefaultJobPersistence(jobDatabase, timeSupplier)
  }

  @AfterEach
  fun tearDown() {
    close(dataSource)
  }

  private fun resetDb() {
    // todo (cgardens) - truncate whole db.
    jobDatabase.query<Int?> { ctx: DSLContext? -> ctx!!.truncateTable(Tables.JOBS).cascade().execute() }
    jobDatabase.query<Int?> { ctx: DSLContext? ->
      ctx!!.truncateTable(Tables.ATTEMPTS).cascade().execute()
    }
    jobDatabase.query<Int?> { ctx: DSLContext? ->
      ctx!!.truncateTable(Tables.AIRBYTE_METADATA).cascade().execute()
    }
    jobDatabase.query<TruncateIdentityStep<SyncStatsRecord?>?> { ctx: DSLContext? ->
      ctx!!.truncateTable(
        Tables.SYNC_STATS,
      )
    }
    jobDatabase.query<TruncateIdentityStep<StreamAttemptMetadataRecord?>?> { ctx: DSLContext? ->
      ctx!!.truncateTable(
        Tables.STREAM_ATTEMPT_METADATA,
      )
    }
  }

  private fun getJobRecord(jobId: Long): Result<Record> =
    jobDatabase.query(
      ContextQueryFunction { ctx: DSLContext ->
        ctx.fetch(
          DefaultJobPersistence.BASE_JOB_SELECT_AND_JOIN + "WHERE jobs.id = ?",
          jobId,
        )
      },
    )

  @Test
  @DisplayName("Should write an attempt failure even if the failure reason contains null bytes")
  fun testWriteAttemptFailureWithNullChars() {
    val jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()
    val attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH)

    jobPersistence.writeAttemptFailureSummary(
      jobId,
      attemptNumber,
      AttemptFailureSummary().withFailures(
        listOf(
          FailureReason()
            .withInternalMessage(
              // kotlin's raw (triple-quoted) strings don't respect backslash escapes.
              // After json-serialization, this will turn into
              // {"internalMessage": "Here are some weird characters: \u0000, \\\u0000, \\\\\u0000, u0000, \\u0000, \\\\u0000, \\\\\\u0000."}
              """Here are some weird characters: ${'\u0000'}, \${'\u0000'}, \\${'\u0000'}, u0000, \u0000, \\u0000, \\\u0000.""",
            ).withStacktrace(
              // regex+string.replace struggles with multiple null chars in sequence, so verify that we can handle this
              """java.time.format.DateTimeParseException: Text '2026-01E${'\u0000'}${'\u0000'}${'\u0000'}-SFATAL${'\u0000'}C08P01${'\u0000'}' could not be parsed at index 7""",
            ),
        ),
      ),
    )

    val persistedAttempt = jobPersistence.getAttemptForJob(jobId, attemptNumber).get()
    assertEquals(
      """Here are some weird characters: <NULL>, <NULL>, <NULL>, u0000, <NULL>, <NULL>, <NULL>.""",
      persistedAttempt.failureSummary!!
        .failures
        .first()
        .internalMessage,
    )
    assertEquals(
      """java.time.format.DateTimeParseException: Text '2026-01E<NULL><NULL><NULL>-SFATAL<NULL>C08P01<NULL>' could not be parsed at index 7""",
      persistedAttempt.failureSummary!!
        .failures
        .first()
        .stacktrace,
    )
  }

  @Test
  @DisplayName("Should set a job to incomplete if an attempt fails")
  fun testCompleteAttemptFailed() {
    val jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()
    val attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH)

    jobPersistence.failAttempt(jobId, attemptNumber)

    val actual = jobPersistence.getJob(jobId)
    val expected =
      createJob(
        jobId,
        SPEC_JOB_CONFIG,
        JobStatus.INCOMPLETE,
        listOf(createAttempt(0, jobId, AttemptStatus.FAILED, LOG_PATH)),
        NOW.epochSecond,
      )
    assertEquals(expected, actual)
  }

  @Test
  @DisplayName("Should set a job to succeeded if an attempt succeeds")
  fun testCompleteAttemptSuccess() {
    val jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()
    val attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH)

    jobPersistence.succeedAttempt(jobId, attemptNumber)

    val actual = jobPersistence.getJob(jobId)
    val expected =
      createJob(
        jobId,
        SPEC_JOB_CONFIG,
        JobStatus.SUCCEEDED,
        listOf(createAttempt(0, jobId, AttemptStatus.SUCCEEDED, LOG_PATH)),
        NOW.epochSecond,
      )
    assertEquals(expected, actual)
  }

  @Test
  @DisplayName("Should be able to read what is written")
  fun testWriteOutput() {
    val jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()
    val attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH)
    val created = jobPersistence.getJob(jobId)
    val syncStats =
      SyncStats()
        .withBytesEmitted(100L)
        .withRecordsEmitted(9L)
        .withRecordsCommitted(10L)
        .withDestinationStateMessagesEmitted(1L)
        .withSourceStateMessagesEmitted(4L)
        .withMaxSecondsBeforeSourceStateMessageEmitted(5L)
        .withMeanSecondsBeforeSourceStateMessageEmitted(2L)
        .withMaxSecondsBetweenStateMessageEmittedandCommitted(10L)
        .withMeanSecondsBetweenStateMessageEmittedandCommitted(3L)
    val streamName = "stream"
    val streamNamespace = "namespace"
    val streamSyncStats =
      StreamSyncStats()
        .withStats(
          SyncStats()
            .withAdditionalStats(mapOf("test-stat" to 123L.toBigDecimal()))
            .withBytesEmitted(100L)
            .withRecordsEmitted(9L)
            .withEstimatedBytes(200L)
            .withEstimatedRecords(10L),
        ).withStreamNamespace(streamNamespace)
        .withStreamName(streamName)

    val standardSyncOutput =
      StandardSyncOutput().withStandardSyncSummary(
        StandardSyncSummary()
          .withTotalStats(syncStats)
          .withStreamStats(listOf(streamSyncStats)),
      )
    val jobOutput = JobOutput().withOutputType(JobOutput.OutputType.DISCOVER_CATALOG).withSync(standardSyncOutput)

    every { timeSupplier.get() } returns Instant.ofEpochMilli(4242)
    jobPersistence.writeOutput(jobId, attemptNumber, jobOutput)

    val updated = jobPersistence.getJob(jobId)

    assertEquals(jobOutput, updated.attempts[0].output)
    assertNotEquals(created.attempts[0].updatedAtInSecond, updated.attempts[0].updatedAtInSecond)

    val attemptStats = jobPersistence.getAttemptStats(jobId, attemptNumber)

    val storedSyncStats = attemptStats.combinedStats
    assertEquals(100L, storedSyncStats!!.bytesEmitted)
    assertEquals(9L, storedSyncStats.recordsEmitted)
    assertEquals(10L, storedSyncStats.recordsCommitted)
    assertEquals(4L, storedSyncStats.sourceStateMessagesEmitted)
    assertEquals(1L, storedSyncStats.destinationStateMessagesEmitted)
    assertEquals(5L, storedSyncStats.maxSecondsBeforeSourceStateMessageEmitted)
    assertEquals(2L, storedSyncStats.meanSecondsBeforeSourceStateMessageEmitted)
    assertEquals(10L, storedSyncStats.maxSecondsBetweenStateMessageEmittedandCommitted)
    assertEquals(3L, storedSyncStats.meanSecondsBetweenStateMessageEmittedandCommitted)

    val storedStreamSyncStats = attemptStats.perStreamStats
    assertEquals(1, storedStreamSyncStats.size)
    assertEquals(streamName, storedStreamSyncStats[0].streamName)
    assertEquals(streamNamespace, storedStreamSyncStats[0].streamNamespace)
    assertEquals(streamSyncStats.stats.bytesEmitted, storedStreamSyncStats[0].stats.bytesEmitted)
    assertEquals(streamSyncStats.stats.recordsEmitted, storedStreamSyncStats[0].stats.recordsEmitted)
    assertEquals(streamSyncStats.stats.estimatedRecords, storedStreamSyncStats[0].stats.estimatedRecords)
    assertEquals(streamSyncStats.stats.estimatedBytes, storedStreamSyncStats[0].stats.estimatedBytes)
    assertEquals(streamSyncStats.stats.additionalStats, storedStreamSyncStats[0].stats.additionalStats)
  }

  @Test
  @DisplayName("Should be able to read what is written when there is a stats upsert")
  fun testWriteOutputWithStreamStatsUpsert() {
    val jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()
    val attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH)
    val created = jobPersistence.getJob(jobId)
    val syncStats1 =
      SyncStats()
        .withBytesEmitted(100L)
        .withRecordsEmitted(9L)
        .withRecordsCommitted(10L)
        .withDestinationStateMessagesEmitted(1L)
        .withSourceStateMessagesEmitted(4L)
        .withMaxSecondsBeforeSourceStateMessageEmitted(5L)
        .withMeanSecondsBeforeSourceStateMessageEmitted(2L)
        .withMaxSecondsBetweenStateMessageEmittedandCommitted(10L)
        .withMeanSecondsBetweenStateMessageEmittedandCommitted(3L)
    val streamName = "stream"
    val streamNamespace = "namespace"
    val streamSyncStats1 =
      StreamSyncStats()
        .withStats(
          SyncStats()
            .withAdditionalStats(mapOf("test-stat" to 123L.toBigDecimal()))
            .withBytesEmitted(100L)
            .withRecordsEmitted(9L)
            .withEstimatedBytes(200L)
            .withEstimatedRecords(10L),
        ).withStreamNamespace(streamNamespace)
        .withStreamName(streamName)

    val streamSyncStats2 =
      StreamSyncStats()
        .withStats(
          SyncStats()
            .withAdditionalStats(mapOf("test-stat" to 246L.toBigDecimal(), "new-stat" to 999L.toBigDecimal()))
            .withBytesEmitted(200L)
            .withRecordsEmitted(18L)
            .withEstimatedBytes(400L)
            .withEstimatedRecords(20L),
        ).withStreamNamespace(streamNamespace)
        .withStreamName(streamName)

    val standardSyncOutput1 =
      StandardSyncOutput().withStandardSyncSummary(
        StandardSyncSummary()
          .withTotalStats(syncStats1)
          .withStreamStats(listOf(streamSyncStats1)),
      )
    val standardSyncOutput2 =
      StandardSyncOutput().withStandardSyncSummary(
        StandardSyncSummary()
          .withTotalStats(syncStats1)
          .withStreamStats(listOf(streamSyncStats2)),
      )
    val jobOutput1 = JobOutput().withOutputType(JobOutput.OutputType.DISCOVER_CATALOG).withSync(standardSyncOutput1)
    val jobOutput2 = JobOutput().withOutputType(JobOutput.OutputType.DISCOVER_CATALOG).withSync(standardSyncOutput2)

    every { timeSupplier.get() } returns Instant.ofEpochMilli(4242)
    jobPersistence.writeOutput(jobId, attemptNumber, jobOutput1)
    jobPersistence.writeOutput(jobId, attemptNumber, jobOutput2)

    val updated = jobPersistence.getJob(jobId)

    assertEquals(jobOutput2, updated.attempts[0].output)
    assertNotEquals(created.attempts[0].updatedAtInSecond, updated.attempts[0].updatedAtInSecond)

    val attemptStats = jobPersistence.getAttemptStats(jobId, attemptNumber)

    val storedSyncStats = attemptStats.combinedStats
    assertEquals(100L, storedSyncStats!!.bytesEmitted)
    assertEquals(9L, storedSyncStats.recordsEmitted)
    assertEquals(10L, storedSyncStats.recordsCommitted)
    assertEquals(4L, storedSyncStats.sourceStateMessagesEmitted)
    assertEquals(1L, storedSyncStats.destinationStateMessagesEmitted)
    assertEquals(5L, storedSyncStats.maxSecondsBeforeSourceStateMessageEmitted)
    assertEquals(2L, storedSyncStats.meanSecondsBeforeSourceStateMessageEmitted)
    assertEquals(10L, storedSyncStats.maxSecondsBetweenStateMessageEmittedandCommitted)
    assertEquals(3L, storedSyncStats.meanSecondsBetweenStateMessageEmittedandCommitted)

    val storedStreamSyncStats = attemptStats.perStreamStats
    assertEquals(1, storedStreamSyncStats.size)
    assertEquals(streamName, storedStreamSyncStats[0].streamName)
    assertEquals(streamNamespace, storedStreamSyncStats[0].streamNamespace)
    assertEquals(streamSyncStats2.stats.bytesEmitted, storedStreamSyncStats[0].stats.bytesEmitted)
    assertEquals(streamSyncStats2.stats.recordsEmitted, storedStreamSyncStats[0].stats.recordsEmitted)
    assertEquals(streamSyncStats2.stats.estimatedRecords, storedStreamSyncStats[0].stats.estimatedRecords)
    assertEquals(streamSyncStats2.stats.estimatedBytes, storedStreamSyncStats[0].stats.estimatedBytes)
    assertEquals(streamSyncStats2.stats.additionalStats, storedStreamSyncStats[0].stats.additionalStats)
  }

  @Test
  @DisplayName("Should be able to add additionalStats via upsert when initially missing")
  fun testWriteOutputWithStreamStatsUpsertNoAdditionalStatsInInsert() {
    val jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()
    val attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH)
    val created = jobPersistence.getJob(jobId)
    val syncStats1 =
      SyncStats()
        .withBytesEmitted(100L)
        .withRecordsEmitted(9L)
        .withRecordsCommitted(10L)
        .withDestinationStateMessagesEmitted(1L)
        .withSourceStateMessagesEmitted(4L)
        .withMaxSecondsBeforeSourceStateMessageEmitted(5L)
        .withMeanSecondsBeforeSourceStateMessageEmitted(2L)
        .withMaxSecondsBetweenStateMessageEmittedandCommitted(10L)
        .withMeanSecondsBetweenStateMessageEmittedandCommitted(3L)
    val streamName = "stream"
    val streamNamespace = "namespace"
    val streamSyncStats1 =
      StreamSyncStats()
        .withStats(
          SyncStats()
            // No additional stats on purpose
            .withBytesEmitted(100L)
            .withRecordsEmitted(9L)
            .withEstimatedBytes(200L)
            .withEstimatedRecords(10L),
        ).withStreamNamespace(streamNamespace)
        .withStreamName(streamName)

    val streamSyncStats2 =
      StreamSyncStats()
        .withStats(
          SyncStats()
            .withAdditionalStats(mapOf("test-stat" to 123L.toBigDecimal()))
            .withBytesEmitted(200L)
            .withRecordsEmitted(18L)
            .withEstimatedBytes(400L)
            .withEstimatedRecords(20L),
        ).withStreamNamespace(streamNamespace)
        .withStreamName(streamName)

    val standardSyncOutput1 =
      StandardSyncOutput().withStandardSyncSummary(
        StandardSyncSummary()
          .withTotalStats(syncStats1)
          .withStreamStats(listOf(streamSyncStats1)),
      )
    val standardSyncOutput2 =
      StandardSyncOutput().withStandardSyncSummary(
        StandardSyncSummary()
          .withTotalStats(syncStats1)
          .withStreamStats(listOf(streamSyncStats2)),
      )
    val jobOutput1 = JobOutput().withOutputType(JobOutput.OutputType.DISCOVER_CATALOG).withSync(standardSyncOutput1)
    val jobOutput2 = JobOutput().withOutputType(JobOutput.OutputType.DISCOVER_CATALOG).withSync(standardSyncOutput2)

    every { timeSupplier.get() } returns Instant.ofEpochMilli(4242)
    jobPersistence.writeOutput(jobId, attemptNumber, jobOutput1)
    jobPersistence.writeOutput(jobId, attemptNumber, jobOutput2)

    val updated = jobPersistence.getJob(jobId)

    assertEquals(jobOutput2, updated.attempts[0].output)
    assertNotEquals(created.attempts[0].updatedAtInSecond, updated.attempts[0].updatedAtInSecond)

    val attemptStats = jobPersistence.getAttemptStats(jobId, attemptNumber)

    val storedSyncStats = attemptStats.combinedStats
    assertEquals(100L, storedSyncStats!!.bytesEmitted)
    assertEquals(9L, storedSyncStats.recordsEmitted)
    assertEquals(10L, storedSyncStats.recordsCommitted)
    assertEquals(4L, storedSyncStats.sourceStateMessagesEmitted)
    assertEquals(1L, storedSyncStats.destinationStateMessagesEmitted)
    assertEquals(5L, storedSyncStats.maxSecondsBeforeSourceStateMessageEmitted)
    assertEquals(2L, storedSyncStats.meanSecondsBeforeSourceStateMessageEmitted)
    assertEquals(10L, storedSyncStats.maxSecondsBetweenStateMessageEmittedandCommitted)
    assertEquals(3L, storedSyncStats.meanSecondsBetweenStateMessageEmittedandCommitted)

    val storedStreamSyncStats = attemptStats.perStreamStats
    assertEquals(1, storedStreamSyncStats.size)
    assertEquals(streamName, storedStreamSyncStats[0].streamName)
    assertEquals(streamNamespace, storedStreamSyncStats[0].streamNamespace)
    assertEquals(streamSyncStats2.stats.bytesEmitted, storedStreamSyncStats[0].stats.bytesEmitted)
    assertEquals(streamSyncStats2.stats.recordsEmitted, storedStreamSyncStats[0].stats.recordsEmitted)
    assertEquals(streamSyncStats2.stats.estimatedRecords, storedStreamSyncStats[0].stats.estimatedRecords)
    assertEquals(streamSyncStats2.stats.estimatedBytes, storedStreamSyncStats[0].stats.estimatedBytes)
    assertEquals(streamSyncStats2.stats.additionalStats, storedStreamSyncStats[0].stats.additionalStats)
  }

  @Test
  @DisplayName("Should be able to read what is written when there is a stats upsert without additional stats in update")
  fun testWriteOutputWithStreamStatsUpsertWithNoAdditionalStatsInUpdate() {
    val jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()
    val attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH)
    val created = jobPersistence.getJob(jobId)
    val syncStats1 =
      SyncStats()
        .withBytesEmitted(100L)
        .withRecordsEmitted(9L)
        .withRecordsCommitted(10L)
        .withDestinationStateMessagesEmitted(1L)
        .withSourceStateMessagesEmitted(4L)
        .withMaxSecondsBeforeSourceStateMessageEmitted(5L)
        .withMeanSecondsBeforeSourceStateMessageEmitted(2L)
        .withMaxSecondsBetweenStateMessageEmittedandCommitted(10L)
        .withMeanSecondsBetweenStateMessageEmittedandCommitted(3L)
    val streamName = "stream"
    val streamNamespace = "namespace"
    val streamSyncStats1 =
      StreamSyncStats()
        .withStats(
          SyncStats()
            .withAdditionalStats(mapOf("test-stat" to 123L.toBigDecimal()))
            .withBytesEmitted(100L)
            .withRecordsEmitted(9L)
            .withEstimatedBytes(200L)
            .withEstimatedRecords(10L),
        ).withStreamNamespace(streamNamespace)
        .withStreamName(streamName)

    val streamSyncStats2 =
      StreamSyncStats()
        .withStats(
          SyncStats()
            // No additional stats on purpose
            .withBytesEmitted(200L)
            .withRecordsEmitted(18L)
            .withEstimatedBytes(400L)
            .withEstimatedRecords(20L),
        ).withStreamNamespace(streamNamespace)
        .withStreamName(streamName)

    val standardSyncOutput1 =
      StandardSyncOutput().withStandardSyncSummary(
        StandardSyncSummary()
          .withTotalStats(syncStats1)
          .withStreamStats(listOf(streamSyncStats1)),
      )
    val standardSyncOutput2 =
      StandardSyncOutput().withStandardSyncSummary(
        StandardSyncSummary()
          .withTotalStats(syncStats1)
          .withStreamStats(listOf(streamSyncStats2)),
      )
    val jobOutput1 = JobOutput().withOutputType(JobOutput.OutputType.DISCOVER_CATALOG).withSync(standardSyncOutput1)
    val jobOutput2 = JobOutput().withOutputType(JobOutput.OutputType.DISCOVER_CATALOG).withSync(standardSyncOutput2)

    every { timeSupplier.get() } returns Instant.ofEpochMilli(4242)
    jobPersistence.writeOutput(jobId, attemptNumber, jobOutput1)
    jobPersistence.writeOutput(jobId, attemptNumber, jobOutput2)

    val updated = jobPersistence.getJob(jobId)

    assertEquals(jobOutput2, updated.attempts[0].output)
    assertNotEquals(created.attempts[0].updatedAtInSecond, updated.attempts[0].updatedAtInSecond)

    val attemptStats = jobPersistence.getAttemptStats(jobId, attemptNumber)

    val storedSyncStats = attemptStats.combinedStats
    assertEquals(100L, storedSyncStats!!.bytesEmitted)
    assertEquals(9L, storedSyncStats.recordsEmitted)
    assertEquals(10L, storedSyncStats.recordsCommitted)
    assertEquals(4L, storedSyncStats.sourceStateMessagesEmitted)
    assertEquals(1L, storedSyncStats.destinationStateMessagesEmitted)
    assertEquals(5L, storedSyncStats.maxSecondsBeforeSourceStateMessageEmitted)
    assertEquals(2L, storedSyncStats.meanSecondsBeforeSourceStateMessageEmitted)
    assertEquals(10L, storedSyncStats.maxSecondsBetweenStateMessageEmittedandCommitted)
    assertEquals(3L, storedSyncStats.meanSecondsBetweenStateMessageEmittedandCommitted)

    val storedStreamSyncStats = attemptStats.perStreamStats
    assertEquals(1, storedStreamSyncStats.size)
    assertEquals(streamName, storedStreamSyncStats[0].streamName)
    assertEquals(streamNamespace, storedStreamSyncStats[0].streamNamespace)
    assertEquals(streamSyncStats2.stats.bytesEmitted, storedStreamSyncStats[0].stats.bytesEmitted)
    assertEquals(streamSyncStats2.stats.recordsEmitted, storedStreamSyncStats[0].stats.recordsEmitted)
    assertEquals(streamSyncStats2.stats.estimatedRecords, storedStreamSyncStats[0].stats.estimatedRecords)
    assertEquals(streamSyncStats2.stats.estimatedBytes, storedStreamSyncStats[0].stats.estimatedBytes)
    // This is explicitly validating that if the second update does not have any additional stats, it does not erase the previously saved value
    assertEquals(streamSyncStats1.stats.additionalStats, storedStreamSyncStats[0].stats.additionalStats)
  }

  @Test
  @DisplayName("Should be able to read AttemptSyncConfig that was written")
  fun testWriteAttemptSyncConfig() {
    val jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()
    val attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH)
    val created = jobPersistence.getJob(jobId)
    val attemptSyncConfig =
      AttemptSyncConfig()
        .withSourceConfiguration(jsonNode(mapOf("source" to "s_config_value")))
        .withDestinationConfiguration(jsonNode(mapOf("destination" to "d_config_value")))
        .withState(State().withState(jsonNode(mapOf("state_key" to "state_value"))))

    every { timeSupplier.get() } returns Instant.ofEpochMilli(4242)
    jobPersistence.writeAttemptSyncConfig(jobId, attemptNumber, attemptSyncConfig)

    val updated = jobPersistence.getJob(jobId)
    assertEquals(attemptSyncConfig, updated.attempts[0].syncConfig)
    assertNotEquals(created.attempts[0].updatedAtInSecond, updated.attempts[0].updatedAtInSecond)
  }

  @Test
  @DisplayName("Should be able to read attemptFailureSummary that was written")
  fun testWriteAttemptFailureSummary() {
    val jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()
    val attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH)
    val created = jobPersistence.getJob(jobId)
    val failureSummary =
      AttemptFailureSummary().withFailures(
        mutableListOf<FailureReason?>(FailureReason().withFailureOrigin(FailureReason.FailureOrigin.SOURCE)),
      )

    every { timeSupplier.get() } returns Instant.ofEpochMilli(4242)
    jobPersistence.writeAttemptFailureSummary(jobId, attemptNumber, failureSummary)

    val updated = jobPersistence.getJob(jobId)
    assertEquals(failureSummary, updated.attempts[0].failureSummary)
    assertNotEquals(created.attempts[0].updatedAtInSecond, updated.attempts[0].updatedAtInSecond)
  }

  @Test
  @DisplayName("Should be able to read attemptFailureSummary that was written with unsupported unicode")
  fun testWriteAttemptFailureSummaryWithUnsupportedUnicode() {
    val jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()
    val attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH)
    val created = jobPersistence.getJob(jobId)
    val failureSummary =
      AttemptFailureSummary().withFailures(
        mutableListOf<FailureReason?>(
          FailureReason()
            .withFailureOrigin(FailureReason.FailureOrigin.SOURCE)
            .withStacktrace(Character.toString(0))
            .withInternalMessage("Includes invalid unicode \u0000")
            .withExternalMessage("Includes invalid unicode \u0000"),
        ),
      )
    every { timeSupplier.get() } returns Instant.ofEpochMilli(4242)
    jobPersistence.writeAttemptFailureSummary(jobId, attemptNumber, failureSummary)

    assertDoesNotThrow {
      val updated = jobPersistence.getJob(jobId)
      assertNotNull(updated.attempts[0].failureSummary)
      assertNotEquals(created.attempts[0].updatedAtInSecond, updated.attempts[0].updatedAtInSecond)
    }
  }

  @Test
  @DisplayName("When getting the last replication job should return the most recently created job")
  fun testGetLastSyncJobWithMultipleAttempts() {
    val jobId = jobPersistence.enqueueJob(SCOPE, SYNC_JOB_CONFIG, true).orElseThrow()
    jobPersistence.failAttempt(jobId, jobPersistence.createAttempt(jobId, LOG_PATH))
    jobPersistence.failAttempt(jobId, jobPersistence.createAttempt(jobId, LOG_PATH))

    val actual = jobPersistence.getLastReplicationJob(UUID.fromString(SCOPE))

    val expected =
      createJob(
        jobId,
        SYNC_JOB_CONFIG,
        JobStatus.INCOMPLETE,
        listOf(
          createAttempt(0, jobId, AttemptStatus.FAILED, LOG_PATH),
          createAttempt(1, jobId, AttemptStatus.FAILED, LOG_PATH),
        ),
        NOW.epochSecond,
      )

    assertEquals(Optional.of(expected), actual)
  }

  @Test
  @DisplayName("When getting the last replication job should return the most recently created job without scheduled functionalities")
  fun testGetLastSyncJobWithCancelWithScheduleEnabledForNonScheduledJob() {
    val jobId = jobPersistence.enqueueJob(SCOPE, SYNC_JOB_CONFIG, false).orElseThrow()
    jobPersistence.failAttempt(jobId, jobPersistence.createAttempt(jobId, LOG_PATH))
    jobPersistence.failAttempt(jobId, jobPersistence.createAttempt(jobId, LOG_PATH))
    jobPersistence.cancelJob(jobId)

    val actual = jobPersistence.getLastReplicationJobWithCancel(UUID.fromString(SCOPE))

    assertEquals(Optional.empty<Any?>(), actual)
  }

  @Test
  @DisplayName("When getting the last replication job should return the most recently created job with scheduled functionalities")
  fun testGetLastSyncJobWithCancelWithScheduleEnabledForScheduledJob() {
    val jobId = jobPersistence.enqueueJob(SCOPE, SYNC_JOB_CONFIG, true).orElseThrow()
    jobPersistence.failAttempt(jobId, jobPersistence.createAttempt(jobId, LOG_PATH))
    jobPersistence.failAttempt(jobId, jobPersistence.createAttempt(jobId, LOG_PATH))
    jobPersistence.cancelJob(jobId)

    val actual = jobPersistence.getLastReplicationJobWithCancel(UUID.fromString(SCOPE))

    val expected: Job =
      createJob(
        jobId,
        SYNC_JOB_CONFIG,
        JobStatus.CANCELLED,
        listOf(
          createAttempt(0, jobId, AttemptStatus.FAILED, LOG_PATH),
          createAttempt(1, jobId, AttemptStatus.FAILED, LOG_PATH),
        ),
        NOW.epochSecond,
        true,
      )
    assertEquals(Optional.of(expected), actual)
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  @DisplayName("Should extract a Job model from a JOOQ result set")
  fun testGetJobFromRecord(isScheduled: Boolean) {
    val jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, isScheduled).orElseThrow()

    val actual = DefaultJobPersistence.getJobFromResult(getJobRecord(jobId))

    val expected = createJob(jobId, SPEC_JOB_CONFIG, JobStatus.PENDING, emptyList(), NOW.epochSecond, isScheduled)
    assertEquals(Optional.of(expected), actual)
  }

  @Test
  @DisplayName("Should extract a Job model from a JOOQ result set")
  fun testGetJobFromRecordDefaultIsScheduled() {
    val jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()

    val actual = DefaultJobPersistence.getJobFromResult(getJobRecord(jobId))

    val expected = createJob(jobId, SPEC_JOB_CONFIG, JobStatus.PENDING, emptyList(), NOW.epochSecond, true)
    assertEquals(Optional.of(expected), actual)
  }

  @Test
  @DisplayName("Should return correct set of jobs when querying on end timestamp")
  fun testListJobsWithTimestamp() {
    // TODO : Once we fix the problem of precision loss in DefaultJobPersistence, change the test value
    // to contain milliseconds as well
    val now = Instant.parse("2021-01-01T00:00:00Z")
    val timeSupplier = incrementingSecondSupplier(now)

    jobPersistence = DefaultJobPersistence(jobDatabase, timeSupplier)
    val syncJobId = jobPersistence.enqueueJob(SCOPE, SYNC_JOB_CONFIG, true).orElseThrow()
    val syncJobAttemptNumber0 = jobPersistence.createAttempt(syncJobId, LOG_PATH)
    jobPersistence.failAttempt(syncJobId, syncJobAttemptNumber0)
    val syncJobSecondAttemptLogPath: Path = LOG_PATH.resolve("2")
    val syncJobAttemptNumber1 = jobPersistence.createAttempt(syncJobId, syncJobSecondAttemptLogPath)
    jobPersistence.failAttempt(syncJobId, syncJobAttemptNumber1)

    val specJobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()
    val specJobAttemptNumber0 = jobPersistence.createAttempt(specJobId, LOG_PATH)
    jobPersistence.failAttempt(specJobId, specJobAttemptNumber0)
    val specJobSecondAttemptLogPath: Path = LOG_PATH.resolve("2")
    val specJobAttemptNumber1 = jobPersistence.createAttempt(specJobId, specJobSecondAttemptLogPath)
    jobPersistence.succeedAttempt(specJobId, specJobAttemptNumber1)

    val jobs = jobPersistence.listJobs(ConfigType.SYNC, Instant.EPOCH)
    assertEquals(jobs.size, 1)
    assertEquals(jobs[0].id, syncJobId)
    assertEquals(jobs[0].attempts.size, 2)
    assertEquals(
      jobs
        [0]
        .attempts
        [0]
        .attemptNumber,
      0,
    )
    assertEquals(
      jobs[0]
        .attempts[1]
        .attemptNumber,
      1,
    )

    val syncJobThirdAttemptLogPath: Path = LOG_PATH.resolve("3")
    val syncJobAttemptNumber2 = jobPersistence.createAttempt(syncJobId, syncJobThirdAttemptLogPath)
    jobPersistence.succeedAttempt(syncJobId, syncJobAttemptNumber2)

    val newSyncJobId = jobPersistence.enqueueJob(SCOPE, SYNC_JOB_CONFIG, true).orElseThrow()
    val newSyncJobAttemptNumber0 = jobPersistence.createAttempt(newSyncJobId, LOG_PATH)
    jobPersistence.failAttempt(newSyncJobId, newSyncJobAttemptNumber0)
    val newSyncJobSecondAttemptLogPath: Path = LOG_PATH.resolve("2")
    val newSyncJobAttemptNumber1 = jobPersistence.createAttempt(newSyncJobId, newSyncJobSecondAttemptLogPath)
    jobPersistence.succeedAttempt(newSyncJobId, newSyncJobAttemptNumber1)

    val maxEndedAtTimestamp =
      jobs[0]
        .attempts
        .stream()
        .map { c: Attempt? -> c!!.endedAtInSecond ?: throw IllegalStateException() }
        .max(
          Comparator { obj: Long?, anotherLong: Long? ->
            obj!!.compareTo(
              anotherLong!!,
            )
          },
        ).orElseThrow()

    val secondQueryJobs = jobPersistence.listJobs(ConfigType.SYNC, Instant.ofEpochSecond(maxEndedAtTimestamp))
    assertEquals(secondQueryJobs.size, 2)
    assertEquals(secondQueryJobs[0].id, syncJobId)
    assertEquals(secondQueryJobs[0].attempts.size, 1)
    assertEquals(
      secondQueryJobs
        [0]
        .attempts
        [0]
        .attemptNumber,
      2,
    )

    assertEquals(secondQueryJobs[1].id, newSyncJobId)
    assertEquals(secondQueryJobs[1].attempts.size, 2)
    assertEquals(
      secondQueryJobs
        [1]
        .attempts
        [0]
        .attemptNumber,
      0,
    )
    assertEquals(
      secondQueryJobs
        [1]
        .attempts
        [1]
        .attemptNumber,
      1,
    )

    var maxEndedAtTimestampAfterSecondQuery = -1L
    for (c in secondQueryJobs) {
      val attempts = c.attempts
      val maxEndedAtTimestampForJob =
        attempts
          .stream()
          .map { attempt: Attempt? -> attempt!!.endedAtInSecond ?: throw IllegalStateException() }
          .max(Comparator { obj: Long?, anotherLong: Long? -> obj!!.compareTo(anotherLong!!) })
          .orElseThrow()
      if (maxEndedAtTimestampForJob > maxEndedAtTimestampAfterSecondQuery) {
        maxEndedAtTimestampAfterSecondQuery = maxEndedAtTimestampForJob
      }
    }

    assertEquals(0, jobPersistence.listJobs(ConfigType.SYNC, Instant.ofEpochSecond(maxEndedAtTimestampAfterSecondQuery)).size)
  }

  @Test
  fun testAirbyteProtocolVersionMaxMetadata() {
    assertTrue(jobPersistence.getAirbyteProtocolVersionMax().isEmpty)

    val maxVersion1 = Version("0.1.0")
    jobPersistence.setAirbyteProtocolVersionMax(maxVersion1)
    val maxVersion1read = jobPersistence.getAirbyteProtocolVersionMax()
    assertEquals(maxVersion1, maxVersion1read.orElseThrow())

    val maxVersion2 = Version("1.2.1")
    jobPersistence.setAirbyteProtocolVersionMax(maxVersion2)
    val maxVersion2read = jobPersistence.getAirbyteProtocolVersionMax()
    assertEquals(maxVersion2, maxVersion2read.orElseThrow())
  }

  @Test
  fun testAirbyteProtocolVersionMinMetadata() {
    assertTrue(jobPersistence.getAirbyteProtocolVersionMin().isEmpty)

    val minVersion1 = Version("1.1.0")
    jobPersistence.setAirbyteProtocolVersionMin(minVersion1)
    val minVersion1read = jobPersistence.getAirbyteProtocolVersionMin()
    assertEquals(minVersion1, minVersion1read.orElseThrow())

    val minVersion2 = Version("3.0.1")
    jobPersistence.setAirbyteProtocolVersionMin(minVersion2)
    val minVersion2read = jobPersistence.getAirbyteProtocolVersionMin()
    assertEquals(minVersion2, minVersion2read.orElseThrow())
  }

  @Test
  fun testAirbyteProtocolVersionRange() {
    val v1 = Version("1.5.0")
    val v2 = Version("2.5.0")
    val range = jobPersistence.getCurrentProtocolVersionRange()
    assertEquals(Optional.empty<Any?>(), range)

    jobPersistence.setAirbyteProtocolVersionMax(v2)
    val range2 = jobPersistence.getCurrentProtocolVersionRange()
    assertEquals(
      Optional.of<AirbyteProtocolVersionRange?>(
        AirbyteProtocolVersionRange(
          AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION,
          v2,
        ),
      ),
      range2,
    )

    jobPersistence.setAirbyteProtocolVersionMin(v1)
    val range3 = jobPersistence.getCurrentProtocolVersionRange()
    assertEquals(Optional.of<AirbyteProtocolVersionRange?>(AirbyteProtocolVersionRange(v1, v2)), range3)
  }

  @Nested
  @DisplayName("Stats Related Tests")
  internal inner class Stats {
    @Test
    @DisplayName("Writing stats the first time should only write record and bytes information correctly")
    fun testWriteStatsFirst() {
      val jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()
      val attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH)
      val additionalStats = mapOf("additional-stat" to 123L.toBigDecimal())
      val streamStats =
        listOf(
          StreamSyncStats()
            .withStreamName("name1")
            .withStreamNamespace("ns")
            .withStats(
              SyncStats()
                .withBytesEmitted(500L)
                .withRecordsEmitted(500L)
                .withEstimatedBytes(10000L)
                .withEstimatedRecords(2000L),
            ),
          StreamSyncStats()
            .withStreamName("name2")
            .withStreamNamespace("ns")
            .withStats(
              SyncStats()
                .withBytesEmitted(500L)
                .withRecordsEmitted(500L)
                .withEstimatedBytes(10000L)
                .withEstimatedRecords(2000L)
                .withAdditionalStats(additionalStats),
            ),
        )
      val estimatedRecords = 1000L
      val estimatedBytes = 1001L
      val recordsEmitted = 1002L
      val bytesEmitted = 1003L
      val recordsCommitted = 1004L
      val bytesCommitted = 1005L
      val recordsRejected = 1006L
      jobPersistence.writeStats(
        jobId,
        attemptNumber,
        estimatedRecords,
        estimatedBytes,
        recordsEmitted,
        bytesEmitted,
        recordsCommitted,
        bytesCommitted,
        recordsRejected,
        CONNECTION_ID,
        streamStats,
      )

      val stats = jobPersistence.getAttemptStats(jobId, attemptNumber)
      val combined = stats.combinedStats
      assertEquals(bytesEmitted, combined!!.bytesEmitted)
      assertEquals(recordsEmitted, combined.recordsEmitted)
      assertEquals(estimatedBytes, combined.estimatedBytes)
      assertEquals(estimatedRecords, combined.estimatedRecords)
      assertEquals(recordsCommitted, combined.recordsCommitted)
      assertEquals(bytesCommitted, combined.bytesCommitted)
      assertEquals(recordsRejected, combined.recordsRejected)

      // As of this writing, committed and state messages are not expected.
      assertNull(combined.destinationStateMessagesEmitted)

      val actStreamStats = stats.perStreamStats
      assertEquals(2, actStreamStats.size)
      assertEquals(streamStats, actStreamStats)
    }

    @Test
    @DisplayName("Fetch stream stats get the default metadata correctly")
    fun testWriteStatsWithMetadataDefault() {
      val jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()
      val attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH)
      val additionalStats = mapOf("test-stat" to 123L.toBigDecimal())
      val streamStats =
        listOf(
          StreamSyncStats()
            .withStreamName("name1")
            .withStreamNamespace("ns")
            .withStats(
              SyncStats()
                .withBytesEmitted(500L)
                .withRecordsEmitted(500L)
                .withEstimatedBytes(10000L)
                .withEstimatedRecords(2000L)
                .withAdditionalStats(additionalStats),
            ).withWasResumed(false)
            .withWasBackfilled(false),
          StreamSyncStats()
            .withStreamName("name2")
            .withStreamNamespace("ns")
            .withStats(
              SyncStats()
                .withBytesEmitted(500L)
                .withRecordsEmitted(500L)
                .withEstimatedBytes(10000L)
                .withEstimatedRecords(2000L)
                .withAdditionalStats(additionalStats),
            ).withWasResumed(false)
            .withWasBackfilled(false),
        )
      val estimatedRecords = 1000L
      val estimatedBytes = 1001L
      val recordsEmitted = 1002L
      val bytesEmitted = 1003L
      val recordsCommitted = 1004L
      val bytesCommitted = 1005L
      val recordsRejected = 1006L
      jobPersistence.writeStats(
        jobId,
        attemptNumber,
        estimatedRecords,
        estimatedBytes,
        recordsEmitted,
        bytesEmitted,
        recordsCommitted,
        bytesCommitted,
        recordsRejected,
        CONNECTION_ID,
        streamStats,
      )

      val stats = jobPersistence.getAttemptStatsWithStreamMetadata(jobId, attemptNumber)

      val actStreamStats = stats.perStreamStats
      assertEquals(2, actStreamStats.size)
      assertEquals(streamStats, actStreamStats)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    @DisplayName("Fetch stream stats get the metadata correctly")
    fun testWriteStatsWithMetadata(resumedBackfilledValue: Boolean) {
      val jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()
      val attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH)
      val streamStats =
        listOf(
          StreamSyncStats()
            .withStreamName("name1")
            .withStats(
              SyncStats()
                .withBytesEmitted(500L)
                .withRecordsEmitted(500L)
                .withEstimatedBytes(10000L)
                .withEstimatedRecords(2000L),
            ).withWasResumed(resumedBackfilledValue)
            .withWasBackfilled(resumedBackfilledValue),
          StreamSyncStats()
            .withStreamName("name2")
            .withStats(
              SyncStats()
                .withBytesEmitted(500L)
                .withRecordsEmitted(500L)
                .withEstimatedBytes(10000L)
                .withEstimatedRecords(2000L),
            ).withWasResumed(resumedBackfilledValue)
            .withWasBackfilled(resumedBackfilledValue),
        )
      val estimatedRecords = 1000L
      val estimatedBytes = 1001L
      val recordsEmitted = 1002L
      val bytesEmitted = 1003L
      val recordsCommitted = 1004L
      val bytesCommitted = 1005L
      val recordsRejected = 1006L
      jobPersistence.writeStats(
        jobId,
        attemptNumber,
        estimatedRecords,
        estimatedBytes,
        recordsEmitted,
        bytesEmitted,
        recordsCommitted,
        bytesCommitted,
        recordsRejected,
        CONNECTION_ID,
        streamStats,
      )

      val attemptId: Long =
        jobDatabase.query { ctx: DSLContext ->
          ctx
            .select(Tables.ATTEMPTS.ID)
            .from(Tables.ATTEMPTS)
            .where(
              Tables.ATTEMPTS.JOB_ID
                .eq(jobId)
                .and(
                  Tables.ATTEMPTS.ATTEMPT_NUMBER
                    .eq(attemptNumber),
                ),
            ).fetchOne(Tables.ATTEMPTS.ID)
        }!!

      jobDatabase.query<Int?> { ctx: DSLContext? ->
        ctx!!
          .insertInto(
            Tables.STREAM_ATTEMPT_METADATA,
            Tables.STREAM_ATTEMPT_METADATA.ID,
            Tables.STREAM_ATTEMPT_METADATA.ATTEMPT_ID,
            Tables.STREAM_ATTEMPT_METADATA.STREAM_NAME,
            Tables.STREAM_ATTEMPT_METADATA.STREAM_NAMESPACE,
            Tables.STREAM_ATTEMPT_METADATA.WAS_BACKFILLED,
            Tables.STREAM_ATTEMPT_METADATA.WAS_RESUMED,
          ).values(UUID.randomUUID(), attemptId, "name1", null, resumedBackfilledValue, resumedBackfilledValue)
          .values(UUID.randomUUID(), attemptId, "name2", null, resumedBackfilledValue, resumedBackfilledValue)
          .execute()
      }

      val stats = jobPersistence.getAttemptStatsWithStreamMetadata(jobId, attemptNumber)

      val actStreamStats = stats.perStreamStats
      assertEquals(2, actStreamStats.size)
      assertEquals(streamStats, actStreamStats)
    }

    @Test
    @DisplayName("Writing stats multiple times should write record and bytes information correctly without exceptions")
    fun testWriteStatsRepeated() {
      val jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()
      val attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH)
      val additionalStats = mapOf("additional-stat" to 123L.toBigDecimal())

      // First write.
      var streamStats =
        listOf(
          StreamSyncStats()
            .withStreamName("name1")
            .withStreamNamespace("ns")
            .withStats(
              SyncStats()
                .withBytesEmitted(500L)
                .withRecordsEmitted(500L)
                .withEstimatedBytes(10000L)
                .withEstimatedRecords(2000L)
                .withAdditionalStats(additionalStats),
            ),
        )
      jobPersistence.writeStats(jobId, attemptNumber, 1000L, 1000L, 1000L, 1000L, 1000L, 1000L, 1000L, CONNECTION_ID, streamStats)

      // Second write.
      every { timeSupplier.get() } returns Instant.now()
      streamStats =
        listOf<StreamSyncStats>(
          StreamSyncStats()
            .withStreamName("name1")
            .withStreamNamespace("ns")
            .withStats(
              SyncStats()
                .withBytesEmitted(1000L)
                .withRecordsEmitted(1000L)
                .withEstimatedBytes(10000L)
                .withEstimatedRecords(2000L)
                .withAdditionalStats(additionalStats),
            ),
        )
      jobPersistence.writeStats(jobId, attemptNumber, 2000L, 2000L, 2000L, 2000L, 2000L, 2000L, 2000L, CONNECTION_ID, streamStats)

      val stats = jobPersistence.getAttemptStats(jobId, attemptNumber)
      val combined = stats.combinedStats
      assertEquals(2000, combined!!.bytesEmitted)
      assertEquals(2000, combined.recordsEmitted)
      assertEquals(2000, combined.estimatedBytes)
      assertEquals(2000, combined.estimatedRecords)
      assertEquals(2000, combined.recordsRejected)

      val actStreamStats = stats.perStreamStats
      assertEquals(1, actStreamStats.size)
      assertEquals(streamStats, actStreamStats)
    }

    @Test
    @DisplayName("Writing multiple stats of the same attempt id, stream name and namespace should update the previous record")
    fun testWriteStatsUpsert() {
      val jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()
      val attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH)
      val additionalStats = mapOf("additional-stat" to 123L.toBigDecimal())

      // First write.
      var streamStats =
        listOf(
          StreamSyncStats()
            .withStreamName("name1")
            .withStreamNamespace("ns")
            .withStats(
              SyncStats()
                .withBytesEmitted(500L)
                .withRecordsEmitted(500L)
                .withEstimatedBytes(10000L)
                .withEstimatedRecords(2000L)
                .withAdditionalStats(additionalStats),
            ),
        )
      jobPersistence.writeStats(jobId, attemptNumber, 1000L, 1000L, 1000L, 1000L, 1000L, 1000L, 1000L, CONNECTION_ID, streamStats)

      // Second write.
      every { timeSupplier.get() } returns Instant.now()
      streamStats =
        listOf<StreamSyncStats>(
          StreamSyncStats()
            .withStreamName("name1")
            .withStreamNamespace("ns")
            .withStats(
              SyncStats()
                .withBytesEmitted(1000L)
                .withRecordsEmitted(1000L)
                .withEstimatedBytes(10000L)
                .withEstimatedRecords(2000L)
                .withAdditionalStats(additionalStats),
            ),
        )
      jobPersistence.writeStats(jobId, attemptNumber, 2000L, 2000L, 2000L, 2000L, 2000L, 2000L, 2000L, CONNECTION_ID, streamStats)

      val syncStatsRec: Record =
        jobDatabase.query<Record?>(
          ContextQueryFunction { ctx: DSLContext ->
            val attemptId: Long = DefaultJobPersistence.getAttemptId(jobId, attemptNumber, ctx)
            ctx
              .fetch("SELECT * from sync_stats where attempt_id = ?", attemptId)
              .stream()
              .findFirst()
              .get()
          },
        )!!

      // Check time stamps to confirm upsert.
      assertNotEquals(
        syncStatsRec.get<OffsetDateTime?>(Tables.SYNC_STATS.CREATED_AT),
        syncStatsRec.get<OffsetDateTime?>(Tables.SYNC_STATS.UPDATED_AT),
      )

      val streamStatsRec: Record =
        jobDatabase.query<Record?>(
          ContextQueryFunction { ctx: DSLContext ->
            val attemptId: Long = DefaultJobPersistence.getAttemptId(jobId, attemptNumber, ctx)
            ctx
              .fetch("SELECT * from stream_stats where attempt_id = ?", attemptId)
              .stream()
              .findFirst()
              .get()
          },
        )!!
      // Check time stamps to confirm upsert.
      assertNotEquals(
        streamStatsRec.get<OffsetDateTime?>(Tables.STREAM_STATS.CREATED_AT),
        streamStatsRec.get<OffsetDateTime?>(Tables.STREAM_STATS.UPDATED_AT),
      )
    }

    @Test
    @DisplayName("Writing multiple stats a stream with null namespace should write correctly without exceptions")
    fun testWriteNullNamespace() {
      val jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()
      val attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH)

      // First write.
      var streamStats =
        listOf(
          StreamSyncStats()
            .withStreamName("name1")
            .withStats(
              SyncStats()
                .withBytesEmitted(500L)
                .withRecordsEmitted(500L)
                .withEstimatedBytes(10000L)
                .withEstimatedRecords(2000L),
            ),
        )
      jobPersistence.writeStats(jobId, attemptNumber, 1000L, 1000L, 1000L, 1000L, 1000L, 1000L, 1000L, CONNECTION_ID, streamStats)

      // Second write.
      every { timeSupplier.get() } returns Instant.now()
      streamStats =
        listOf<StreamSyncStats>(
          StreamSyncStats()
            .withStreamName("name1")
            .withStats(
              SyncStats()
                .withBytesEmitted(1000L)
                .withRecordsEmitted(1000L)
                .withEstimatedBytes(10000L)
                .withEstimatedRecords(2000L),
            ),
        )
      jobPersistence.writeStats(jobId, attemptNumber, 2000L, 2000L, 2000L, 2000L, 2000L, 2000L, 2000L, CONNECTION_ID, streamStats)

      val stats = jobPersistence.getAttemptStats(jobId, attemptNumber)
      val combined = stats.combinedStats
      assertEquals(2000, combined!!.bytesEmitted)
      assertEquals(2000, combined.recordsEmitted)
      assertEquals(2000, combined.estimatedBytes)
      assertEquals(2000, combined.estimatedRecords)

      val actStreamStats = stats.perStreamStats
      assertEquals(1, actStreamStats.size)
      assertEquals(streamStats, actStreamStats)
    }

    @Test
    @DisplayName("Writing multiple stats a stream with null namespace should write correctly without exceptions")
    fun testGetStatsNoResult() {
      val jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()
      val attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH)

      val stats = jobPersistence.getAttemptStats(jobId, attemptNumber)
      assertNull(stats.combinedStats)
      assertEquals(0, stats.perStreamStats.size)
    }

    @Test
    @DisplayName("Retrieving all attempts stats for a job should return the right information")
    fun testGetMultipleStats() {
      val jobOneId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()
      val jobOneAttemptNumberOne = jobPersistence.createAttempt(jobOneId, LOG_PATH)
      val additionalStats = mapOf("test-stat" to 123L.toBigDecimal())

      // First write for first attempt.
      var streamStats =
        listOf(
          StreamSyncStats()
            .withStreamName("name1")
            .withStats(
              SyncStats()
                .withBytesEmitted(1L)
                .withRecordsEmitted(1L)
                .withEstimatedBytes(2L)
                .withEstimatedRecords(2L)
                .withAdditionalStats(additionalStats),
            ),
          StreamSyncStats()
            .withStreamName("name2")
            .withStreamNamespace("ns")
            .withStats(
              SyncStats()
                .withBytesEmitted(1L)
                .withRecordsEmitted(1L)
                .withEstimatedBytes(2L)
                .withEstimatedRecords(2L)
                .withAdditionalStats(additionalStats),
            ),
        )
      jobPersistence.writeStats(jobOneId, jobOneAttemptNumberOne, 1000L, 1000L, 1000L, 1000L, 1000L, 1000L, 1000L, CONNECTION_ID, streamStats)

      // Second write for first attempt. This is the record that should be returned.
      every { timeSupplier.get() } returns Instant.now()
      streamStats =
        listOf<StreamSyncStats>(
          StreamSyncStats()
            .withStreamName("name1")
            .withStats(
              SyncStats()
                .withBytesEmitted(100L)
                .withRecordsEmitted(10L)
                .withEstimatedBytes(200L)
                .withEstimatedRecords(20L)
                .withBytesCommitted(100L)
                .withRecordsCommitted(10L)
                .withRecordsRejected(1L)
                .withAdditionalStats(additionalStats),
            ),
          StreamSyncStats()
            .withStreamName("name2")
            .withStreamNamespace("ns")
            .withStats(
              SyncStats()
                .withBytesEmitted(1000L)
                .withRecordsEmitted(100L)
                .withEstimatedBytes(2000L)
                .withEstimatedRecords(200L)
                .withBytesCommitted(888L)
                .withRecordsCommitted(88L)
                .withRecordsRejected(8L)
                .withAdditionalStats(additionalStats),
            ),
        )
      jobPersistence.writeStats(jobOneId, jobOneAttemptNumberOne, 220L, 2200L, 110L, 1100L, 98L, 988L, 9L, CONNECTION_ID, streamStats)
      jobPersistence.failAttempt(jobOneId, jobOneAttemptNumberOne)

      // Second attempt for first job.
      streamStats =
        listOf<StreamSyncStats>(
          StreamSyncStats()
            .withStreamName("name1")
            .withStats(
              SyncStats()
                .withBytesEmitted(1000L)
                .withRecordsEmitted(100L)
                .withEstimatedBytes(2000L)
                .withEstimatedRecords(200L)
                .withBytesCommitted(1000L)
                .withRecordsCommitted(100L)
                .withRecordsRejected(10L)
                .withAdditionalStats(additionalStats),
            ),
          StreamSyncStats()
            .withStreamName("name2")
            .withStreamNamespace("ns")
            .withStats(
              SyncStats()
                .withBytesEmitted(10000L)
                .withRecordsEmitted(1000L)
                .withEstimatedBytes(20000L)
                .withEstimatedRecords(2000L)
                .withBytesCommitted(8880L)
                .withRecordsCommitted(880L)
                .withRecordsRejected(80L)
                .withAdditionalStats(additionalStats),
            ),
        )
      val jobOneAttemptNumberTwo = jobPersistence.createAttempt(jobOneId, LOG_PATH)
      jobPersistence.writeStats(jobOneId, jobOneAttemptNumberTwo, 1000L, 1000L, 1000L, 1000L, 1000L, 1000L, 1000L, CONNECTION_ID, streamStats)

      // First attempt for second job.
      val jobTwoId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()
      val jobTwoAttemptNumberOne = jobPersistence.createAttempt(jobTwoId, LOG_PATH)
      streamStats =
        listOf<StreamSyncStats>(
          StreamSyncStats()
            .withStreamName("name1")
            .withStats(
              SyncStats()
                .withBytesEmitted(1000L)
                .withRecordsEmitted(1000L)
                .withEstimatedBytes(10000L)
                .withEstimatedRecords(2000L)
                .withAdditionalStats(additionalStats),
            ),
          StreamSyncStats()
            .withStreamName("name2")
            .withStreamNamespace("ns")
            .withStats(
              SyncStats()
                .withBytesEmitted(5000L)
                .withRecordsEmitted(5000L)
                .withEstimatedBytes(100000L)
                .withEstimatedRecords(20000L)
                .withAdditionalStats(additionalStats),
            ),
        )
      jobPersistence.writeStats(jobTwoId, jobTwoAttemptNumberOne, 1000L, 1000L, 1000L, 1000L, 1000L, 1000L, 1000L, CONNECTION_ID, streamStats)

      val jobOneAttemptIds: MutableList<Long?> =
        jobDatabase.query<MutableList<Long?>?> { ctx: DSLContext ->
          ctx
            .select(Tables.ATTEMPTS.ID)
            .from(Tables.ATTEMPTS)
            .where(
              Tables.ATTEMPTS.JOB_ID
                .eq(jobOneId),
            ).orderBy(Tables.ATTEMPTS.ID)
            .fetch()
            .map<Long?>(
              org.jooq.RecordMapper { r: org.jooq.Record1<Long?> ->
                r.get(Tables.ATTEMPTS.ID)
              },
            )
        }!!
      val jobTwoAttemptIds: MutableList<Long?> =
        jobDatabase.query<MutableList<Long?>?> { ctx: DSLContext ->
          ctx
            .select(Tables.ATTEMPTS.ID)
            .from(Tables.ATTEMPTS)
            .where(
              Tables.ATTEMPTS.JOB_ID
                .eq(jobTwoId),
            ).orderBy(Tables.ATTEMPTS.ID)
            .fetch()
            .map<Long?>(
              org.jooq.RecordMapper { r: org.jooq.Record1<Long?> ->
                r.get(Tables.ATTEMPTS.ID)
              },
            )
        }!!
      jobDatabase.query<Int?> { ctx: DSLContext? ->
        ctx!!
          .insertInto(
            Tables.STREAM_ATTEMPT_METADATA,
            Tables.STREAM_ATTEMPT_METADATA.ID,
            Tables.STREAM_ATTEMPT_METADATA.ATTEMPT_ID,
            Tables.STREAM_ATTEMPT_METADATA.STREAM_NAME,
            Tables.STREAM_ATTEMPT_METADATA.STREAM_NAMESPACE,
            Tables.STREAM_ATTEMPT_METADATA.WAS_BACKFILLED,
            Tables.STREAM_ATTEMPT_METADATA.WAS_RESUMED,
          ).values(UUID.randomUUID(), jobOneAttemptIds[0], "name1", null, true, false)
          .values(UUID.randomUUID(), jobOneAttemptIds[1], "name1", null, false, true)
          .values(UUID.randomUUID(), jobTwoAttemptIds[0], "name2", "ns", true, false)
          .execute()
      }

      val stats = jobPersistence.getAttemptStats(listOf(jobOneId, jobTwoId))
      val exp =
        mapOf(
          JobAttemptPair(jobOneId, jobOneAttemptNumberOne) to
            JobPersistence.AttemptStats(
              SyncStats()
                .withBytesEmitted(1100L)
                .withRecordsEmitted(110L)
                .withEstimatedBytes(2200L)
                .withEstimatedRecords(220L)
                .withBytesCommitted(988L)
                .withRecordsCommitted(98L)
                .withRecordsRejected(9L)
                .withAdditionalStats(emptyMap()),
              listOf(
                StreamSyncStats()
                  .withStreamName("name1")
                  .withStats(
                    SyncStats()
                      .withBytesEmitted(100L)
                      .withRecordsEmitted(10L)
                      .withEstimatedBytes(200L)
                      .withEstimatedRecords(20L)
                      .withBytesCommitted(100L)
                      .withRecordsCommitted(10L)
                      .withRecordsRejected(1L)
                      .withAdditionalStats(additionalStats),
                  ).withWasBackfilled(true)
                  .withWasResumed(false),
                StreamSyncStats()
                  .withStreamName("name2")
                  .withStreamNamespace("ns")
                  .withStats(
                    SyncStats()
                      .withBytesEmitted(1000L)
                      .withRecordsEmitted(100L)
                      .withEstimatedBytes(2000L)
                      .withEstimatedRecords(200L)
                      .withBytesCommitted(888L)
                      .withRecordsCommitted(88L)
                      .withRecordsRejected(8L)
                      .withAdditionalStats(additionalStats),
                  ).withWasBackfilled(false)
                  .withWasResumed(false),
              ),
            ),
          JobAttemptPair(jobOneId, jobOneAttemptNumberTwo) to
            JobPersistence.AttemptStats(
              SyncStats()
                .withRecordsEmitted(1000L)
                .withBytesEmitted(1000L)
                .withEstimatedBytes(1000L)
                .withEstimatedRecords(1000L)
                .withBytesCommitted(1000L)
                .withRecordsCommitted(1000L)
                .withRecordsRejected(1000L)
                .withAdditionalStats(emptyMap()),
              listOf(
                StreamSyncStats()
                  .withStreamName("name1")
                  .withStats(
                    SyncStats()
                      .withBytesEmitted(1000L)
                      .withRecordsEmitted(100L)
                      .withEstimatedBytes(2000L)
                      .withEstimatedRecords(200L)
                      .withBytesCommitted(1000L)
                      .withRecordsCommitted(100L)
                      .withRecordsRejected(10L)
                      .withAdditionalStats(additionalStats),
                  ).withWasBackfilled(false)
                  .withWasResumed(true),
                StreamSyncStats()
                  .withStreamName("name2")
                  .withStreamNamespace("ns")
                  .withStats(
                    SyncStats()
                      .withBytesEmitted(10000L)
                      .withRecordsEmitted(1000L)
                      .withEstimatedBytes(20000L)
                      .withEstimatedRecords(2000L)
                      .withBytesCommitted(8880L)
                      .withRecordsCommitted(880L)
                      .withRecordsRejected(80L)
                      .withAdditionalStats(additionalStats),
                  ).withWasBackfilled(false)
                  .withWasResumed(false),
              ),
            ),
          JobAttemptPair(jobTwoId, jobTwoAttemptNumberOne) to
            JobPersistence.AttemptStats(
              SyncStats()
                .withRecordsEmitted(1000L)
                .withBytesEmitted(1000L)
                .withEstimatedBytes(1000L)
                .withEstimatedRecords(1000L)
                .withBytesCommitted(1000L)
                .withRecordsCommitted(1000L)
                .withRecordsRejected(1000L)
                .withAdditionalStats(emptyMap()),
              listOf(
                StreamSyncStats()
                  .withStreamName("name1")
                  .withStats(
                    SyncStats()
                      .withBytesEmitted(1000L)
                      .withRecordsEmitted(1000L)
                      .withEstimatedBytes(10000L)
                      .withEstimatedRecords(2000L)
                      .withAdditionalStats(additionalStats),
                  ).withWasBackfilled(false)
                  .withWasResumed(false),
                StreamSyncStats()
                  .withStreamName("name2")
                  .withStreamNamespace("ns")
                  .withStats(
                    SyncStats()
                      .withEstimatedBytes(100000L)
                      .withEstimatedRecords(20000L)
                      .withBytesEmitted(5000L)
                      .withRecordsEmitted(5000L)
                      .withAdditionalStats(additionalStats),
                  ).withWasBackfilled(true)
                  .withWasResumed(false),
              ),
            ),
        )

      assertEquals(canonicalJsonSerialize(exp), canonicalJsonSerialize(stats))
    }

    @Test
    @DisplayName("Writing stats for different streams should not have side effects")
    fun testWritingStatsForDifferentStreams() {
      val jobOneId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()
      val jobOneAttemptNumberOne = jobPersistence.createAttempt(jobOneId, LOG_PATH)
      val additionalStats = mapOf("test-stat" to 123L.toBigDecimal())

      val stream1 = "s1"
      val namespace1 = "ns1"
      val stream2 = "s2"
      val namespace2 = "ns2"
      val stream3 = "s3"
      val namespace3: String? = null

      val streamStatsUpdate0 =
        listOf(
          StreamSyncStats()
            .withStreamName(stream1)
            .withStreamNamespace(namespace1)
            .withStats(SyncStats().withBytesEmitted(0L).withRecordsEmitted(0L).withAdditionalStats(additionalStats)),
          StreamSyncStats()
            .withStreamName(stream2)
            .withStreamNamespace(namespace2)
            .withStats(SyncStats().withBytesEmitted(0L).withRecordsEmitted(0L).withAdditionalStats(additionalStats)),
          StreamSyncStats()
            .withStreamName(stream3)
            .withStreamNamespace(namespace3)
            .withStats(SyncStats().withBytesEmitted(0L).withRecordsEmitted(0L).withAdditionalStats(additionalStats)),
        )
      jobPersistence.writeStats(
        jobOneId,
        jobOneAttemptNumberOne,
        null,
        null,
        null,
        null,
        1000L,
        null,
        null,
        CONNECTION_ID,
        streamStatsUpdate0,
      )

      val updatedAdditionalStats = mapOf("test-stat" to 246L.toBigDecimal())
      val streamStatsUpdate1 =
        listOf(
          StreamSyncStats()
            .withStreamName(stream1)
            .withStreamNamespace(namespace1)
            .withStats(SyncStats().withBytesEmitted(10L).withRecordsEmitted(1L).withAdditionalStats(updatedAdditionalStats))
            .withWasBackfilled(false)
            .withWasResumed(false),
        )
      jobPersistence.writeStats(jobOneId, jobOneAttemptNumberOne, null, null, 1L, 10L, 1000L, null, null, CONNECTION_ID, streamStatsUpdate1)

      val streamStatsUpdate2 =
        listOf(
          StreamSyncStats()
            .withStreamName(stream2)
            .withStreamNamespace(namespace2)
            .withStats(SyncStats().withBytesEmitted(20L).withRecordsEmitted(2L).withAdditionalStats(updatedAdditionalStats))
            .withWasBackfilled(false)
            .withWasResumed(false),
        )
      jobPersistence.writeStats(jobOneId, jobOneAttemptNumberOne, null, null, 3L, 30L, 1000L, null, null, CONNECTION_ID, streamStatsUpdate2)

      val streamStatsUpdate3 =
        listOf(
          StreamSyncStats()
            .withStreamName(stream3)
            .withStreamNamespace(namespace3)
            .withStats(SyncStats().withBytesEmitted(30L).withRecordsEmitted(3L).withAdditionalStats(updatedAdditionalStats))
            .withWasBackfilled(false)
            .withWasResumed(false),
        )
      jobPersistence.writeStats(jobOneId, jobOneAttemptNumberOne, null, null, 6L, 60L, 1000L, null, null, CONNECTION_ID, streamStatsUpdate3)

      val stats = jobPersistence.getAttemptStats(listOf(jobOneId))
      val attempt1Stats: JobPersistence.AttemptStats = stats[JobAttemptPair(jobOneId, jobOneAttemptNumberOne)]!!

      val actualStreamSyncStats1 = getStreamSyncStats(attempt1Stats, stream1, namespace1)
      assertEquals(streamStatsUpdate1, actualStreamSyncStats1)
      val actualStreamSyncStats2 = getStreamSyncStats(attempt1Stats, stream2, namespace2)
      assertEquals(streamStatsUpdate2, actualStreamSyncStats2)
      val actualStreamSyncStats3 = getStreamSyncStats(attempt1Stats, stream3, namespace3)
      assertEquals(streamStatsUpdate3, actualStreamSyncStats3)
    }

    private fun getStreamSyncStats(
      attemptStats: JobPersistence.AttemptStats,
      streamName: String?,
      namespace: String?,
    ): MutableList<StreamSyncStats?> =
      attemptStats.perStreamStats
        .stream()
        .filter { s: StreamSyncStats? -> s!!.streamName == streamName && (namespace == null || s.streamNamespace == namespace) }
        .toList()

    @Test
    @DisplayName("Retrieving stats for an empty list should not cause an exception.")
    fun testGetStatsForEmptyJobList() {
      assertNotNull(jobPersistence.getAttemptStats(mutableListOf()))
    }

    @Test
    @DisplayName("Retrieving stats for a bad job attempt input should not cause an exception.")
    fun testGetStatsForBadJobAttemptInput() {
      assertNotNull(jobPersistence.getAttemptStats(-1, -1))
    }

    @Test
    @DisplayName("Combined stats can be retrieved without per stream stats.")
    fun testGetAttemptCombinedStats() {
      val jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()
      val attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH)
      val estimatedRecords = 1234L
      val estimatedBytes = 5678L
      val recordsEmitted = 9012L
      val bytesEmitted = 3456L
      val recordsCommitted = 7890L
      val bytesCommitted = 1234L
      val recordsRejected = 5678L

      val streamStats =
        listOf(
          StreamSyncStats()
            .withStreamName("name1")
            .withStreamNamespace("ns")
            .withStats(
              SyncStats()
                .withBytesEmitted(500L)
                .withRecordsEmitted(500L)
                .withEstimatedBytes(10000L)
                .withEstimatedRecords(2000L),
            ),
        )
      jobPersistence.writeStats(
        jobId,
        attemptNumber,
        estimatedRecords,
        estimatedBytes,
        recordsEmitted,
        bytesEmitted,
        recordsCommitted,
        bytesCommitted,
        recordsRejected,
        CONNECTION_ID,
        streamStats,
      )

      val stats = jobPersistence.getAttemptCombinedStats(jobId, attemptNumber)
      assertEquals(estimatedRecords, stats!!.estimatedRecords)
      assertEquals(estimatedBytes, stats.estimatedBytes)
      assertEquals(recordsEmitted, stats.recordsEmitted)
      assertEquals(bytesEmitted, stats.bytesEmitted)
      assertEquals(recordsCommitted, stats.recordsCommitted)
      assertEquals(bytesCommitted, stats.bytesCommitted)
      assertEquals(recordsRejected, stats.recordsRejected)
    }
  }

  @Nested
  internal inner class GetAndSetVersion {
    @Test
    fun testSetVersion() {
      val version = UUID.randomUUID().toString()
      jobPersistence.setVersion(version)
      assertEquals(version, jobPersistence.getVersion().orElseThrow())
    }

    @Test
    fun testSetVersionReplacesExistingId() {
      val deploymentId1 = UUID.randomUUID().toString()
      val deploymentId2 = UUID.randomUUID().toString()
      jobPersistence.setVersion(deploymentId1)
      jobPersistence.setVersion(deploymentId2)
      assertEquals(deploymentId2, jobPersistence.getVersion().orElseThrow())
    }
  }

  @Nested
  internal inner class GetAndSetDeployment {
    @Test
    fun testSetDeployment() {
      val deploymentId = UUID.randomUUID()
      jobPersistence.setDeployment(deploymentId)
      assertEquals(deploymentId, jobPersistence.getDeployment().orElseThrow())
    }

    @Test
    fun testSetDeploymentIdDoesNotReplaceExistingId() {
      val deploymentId1 = UUID.randomUUID()
      val deploymentId2 = UUID.randomUUID()
      jobPersistence.setDeployment(deploymentId1)
      jobPersistence.setDeployment(deploymentId2)
      assertEquals(deploymentId1, jobPersistence.getDeployment().orElseThrow())
    }
  }

  @Nested
  @DisplayName("When cancelling job")
  internal inner class CancelJob {
    @Test
    @DisplayName("Should cancel job and leave job in cancelled state")
    fun testCancelJob() {
      val jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()
      val created = jobPersistence.getJob(jobId)

      every { timeSupplier.get() } returns Instant.ofEpochMilli(4242)
      jobPersistence.cancelJob(jobId)

      val updated = jobPersistence.getJob(jobId)
      assertEquals(JobStatus.CANCELLED, updated.status)
      assertNotEquals(created.updatedAtInSecond, updated.updatedAtInSecond)
    }

    @Test
    @DisplayName("Should not raise an exception if job is already succeeded")
    fun testCancelJobAlreadySuccessful() {
      val jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()
      val attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH)
      jobPersistence.succeedAttempt(jobId, attemptNumber)

      assertDoesNotThrow { jobPersistence.cancelJob(jobId) }

      val updated = jobPersistence.getJob(jobId)
      assertEquals(JobStatus.SUCCEEDED, updated.status)
    }
  }

  @Nested
  @DisplayName("When creating attempt")
  internal inner class CreateAttempt {
    @Test
    @DisplayName("Should create an attempt")
    fun testCreateAttempt() {
      val jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()
      jobPersistence.createAttempt(jobId, LOG_PATH)

      val actual = jobPersistence.getJob(jobId)
      val expected =
        createJob(
          jobId,
          SPEC_JOB_CONFIG,
          JobStatus.RUNNING,
          listOf(createUnfinishedAttempt(0, jobId, AttemptStatus.RUNNING, LOG_PATH)),
          NOW.epochSecond,
        )
      assertEquals(expected, actual)
    }

    @Test
    @DisplayName("Should increment attempt id if creating multiple attempts")
    fun testCreateAttemptAttemptId() {
      val jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()
      val attemptNumber1 = jobPersistence.createAttempt(jobId, LOG_PATH)
      jobPersistence.failAttempt(jobId, attemptNumber1)

      val jobAfterOneAttempts = jobPersistence.getJob(jobId)
      assertEquals(0, attemptNumber1)
      assertEquals(0, jobAfterOneAttempts.attempts[0].attemptNumber)

      val attemptNumber2 = jobPersistence.createAttempt(jobId, LOG_PATH)
      val jobAfterTwoAttempts = jobPersistence.getJob(jobId)
      assertEquals(1, attemptNumber2)
      assertEquals(
        setOf(0, 1),
        jobAfterTwoAttempts.attempts.map { it.attemptNumber }.toSet(),
      )
    }

    @Test
    @DisplayName("Should not create an attempt if an attempt is running")
    fun testCreateAttemptWhileAttemptAlreadyRunning() {
      val jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()
      jobPersistence.createAttempt(jobId, LOG_PATH)

      assertThrows(
        IllegalStateException::class.java,
      ) { jobPersistence.createAttempt(jobId, LOG_PATH) }

      val actual = jobPersistence.getJob(jobId)
      val expected =
        createJob(
          jobId,
          SPEC_JOB_CONFIG,
          JobStatus.RUNNING,
          listOf(createUnfinishedAttempt(0, jobId, AttemptStatus.RUNNING, LOG_PATH)),
          NOW.epochSecond,
        )
      assertEquals(expected, actual)
    }

    @Test
    @DisplayName("Should not create an attempt if job is in terminal state")
    fun testCreateAttemptTerminal() {
      val jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()
      val attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH)
      jobPersistence.succeedAttempt(jobId, attemptNumber)

      assertThrows(
        IllegalStateException::class.java,
      ) { jobPersistence.createAttempt(jobId, LOG_PATH) }

      val actual = jobPersistence.getJob(jobId)
      val expected =
        createJob(
          jobId,
          SPEC_JOB_CONFIG,
          JobStatus.SUCCEEDED,
          listOf(createAttempt(0, jobId, AttemptStatus.SUCCEEDED, LOG_PATH)),
          NOW.epochSecond,
        )
      assertEquals(expected, actual)
    }
  }

  @Nested
  @DisplayName("Get an attempt")
  internal inner class GetAttempt {
    @Test
    @DisplayName("Should get an attempt by job id")
    fun testGetAttemptSimple() {
      val jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()
      val num = jobPersistence.createAttempt(jobId, LOG_PATH)

      val actual = jobPersistence.getAttemptForJob(jobId, 0).get()
      val expected: Attempt = createUnfinishedAttempt(num, jobId, AttemptStatus.RUNNING, LOG_PATH)

      assertEquals(expected, actual)
    }

    @Test
    @DisplayName("Should get an attempt specified by attempt number")
    fun testGetAttemptMultiple() {
      val jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()

      for (i in 0..9) {
        val attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH)
        assertEquals(attemptNumber, i)

        val running = jobPersistence.getAttemptForJob(jobId, attemptNumber).get()
        val expectedRunning: Attempt = createUnfinishedAttempt(attemptNumber, jobId, AttemptStatus.RUNNING, LOG_PATH)
        assertEquals(expectedRunning, running)

        jobPersistence.failAttempt(jobId, attemptNumber)

        val failed = jobPersistence.getAttemptForJob(jobId, attemptNumber).get()
        val expectedFailed: Attempt = createAttempt(attemptNumber, jobId, AttemptStatus.FAILED, LOG_PATH)
        assertEquals(expectedFailed, failed)
      }

      val last = jobPersistence.createAttempt(jobId, LOG_PATH)

      val running = jobPersistence.getAttemptForJob(jobId, last).get()
      val expectedRunning: Attempt = createUnfinishedAttempt(last, jobId, AttemptStatus.RUNNING, LOG_PATH)
      assertEquals(expectedRunning, running)

      jobPersistence.succeedAttempt(jobId, last)

      val succeeded = jobPersistence.getAttemptForJob(jobId, last).get()
      val expectedFailed: Attempt = createAttempt(last, jobId, AttemptStatus.SUCCEEDED, LOG_PATH)
      assertEquals(expectedFailed, succeeded)
    }
  }

  @Nested
  @DisplayName("List attempts after a given timestamp for a given connection")
  internal inner class ListAttemptsByConnectionByTimestamp {
    @Test
    @DisplayName("Returns only entries after the timestamp")
    fun testListAttemptsForConnectionAfterTimestamp() {
      val jobId1 = jobPersistence.enqueueJob(SCOPE, SYNC_JOB_CONFIG, true).orElseThrow()
      val attemptId1 = jobPersistence.createAttempt(jobId1, LOG_PATH)
      jobPersistence.succeedAttempt(jobId1, attemptId1)

      val addTwoSeconds: Instant = NOW.plusSeconds(2)
      every { timeSupplier.get() } returns addTwoSeconds
      val afterNow: Instant = NOW

      val jobId2 = jobPersistence.enqueueJob(SCOPE, SYNC_JOB_CONFIG, true).orElseThrow()
      val attemptId2 = jobPersistence.createAttempt(jobId2, LOG_PATH)
      jobPersistence.succeedAttempt(jobId2, attemptId2)

      val jobId3 = jobPersistence.enqueueJob(SCOPE, SYNC_JOB_CONFIG, true).orElseThrow()
      val attemptId3 = jobPersistence.createAttempt(jobId3, LOG_PATH)
      jobPersistence.succeedAttempt(jobId3, attemptId3)

      val attempts =
        jobPersistence.listAttemptsForConnectionAfterTimestamp(
          CONNECTION_ID,
          ConfigType.SYNC,
          afterNow,
        )

      assertEquals(2, attempts.size)
      assertEquals(jobId2, attempts[0].jobInfo.id)
      assertEquals(jobId3, attempts[1].jobInfo.id)
    }
  }

  @Nested
  @DisplayName("When enqueueing job")
  internal inner class EnqueueJob {
    @Test
    @DisplayName("Should create initial job without attempt")
    fun testCreateJobAndGetWithoutAttemptJob() {
      val jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()

      val actual = jobPersistence.getJob(jobId)
      val expected = createJob(jobId, SPEC_JOB_CONFIG, JobStatus.PENDING, emptyList(), NOW.epochSecond)
      assertEquals(expected, actual)
    }

    @Test
    @DisplayName("Should not create a second job if a job under the same scope is in a non-terminal state")
    fun testCreateJobNoQueueing() {
      val jobId1 = jobPersistence.enqueueJob(SCOPE, SYNC_JOB_CONFIG, true)
      val jobId2 = jobPersistence.enqueueJob(SCOPE, SYNC_JOB_CONFIG, true)

      assertTrue(jobId1.isPresent)
      assertTrue(jobId2.isEmpty)

      val actual = jobPersistence.getJob(jobId1.get())
      val expected = createJob(jobId1.get(), SYNC_JOB_CONFIG, JobStatus.PENDING, emptyList(), NOW.epochSecond)
      assertEquals(expected, actual)
    }

    @Test
    @DisplayName("Should create a second job if a previous job under the same scope has failed")
    fun testCreateJobIfPrevJobFailed() {
      val jobId1 = jobPersistence.enqueueJob(SCOPE, SYNC_JOB_CONFIG, true)
      assertTrue(jobId1.isPresent)

      jobPersistence.failJob(jobId1.get())
      val jobId2 = jobPersistence.enqueueJob(SCOPE, SYNC_JOB_CONFIG, true)
      assertTrue(jobId2.isPresent)

      val actual = jobPersistence.getJob(jobId2.get())
      val expected = createJob(jobId2.get(), SYNC_JOB_CONFIG, JobStatus.PENDING, emptyList(), NOW.epochSecond)
      assertEquals(expected, actual)
    }
  }

  @Nested
  @DisplayName("When failing job")
  internal inner class FailJob {
    @Test
    @DisplayName("Should set job status to failed")
    fun failJob() {
      val jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()
      val created = jobPersistence.getJob(jobId)

      every { timeSupplier.get() } returns Instant.ofEpochMilli(4242)
      jobPersistence.failJob(jobId)

      val updated = jobPersistence.getJob(jobId)
      assertEquals(JobStatus.FAILED, updated.status)
      assertNotEquals(created.updatedAtInSecond, updated.updatedAtInSecond)
    }

    @Test
    @DisplayName("Should not raise an exception if job is already succeeded")
    fun testFailJobAlreadySucceeded() {
      val jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()
      val attemptNumber = jobPersistence.createAttempt(jobId, LOG_PATH)
      jobPersistence.succeedAttempt(jobId, attemptNumber)

      assertDoesNotThrow { jobPersistence.failJob(jobId) }

      val updated = jobPersistence.getJob(jobId)
      assertEquals(JobStatus.SUCCEEDED, updated.status)
    }
  }

  @Nested
  @DisplayName("When getting last replication job")
  internal inner class GetLastReplicationJob {
    @Test
    @DisplayName("Should return nothing if no job exists")
    fun testGetLastReplicationJobForConnectionIdEmpty() {
      val actual = jobPersistence.getLastReplicationJob(CONNECTION_ID)

      assertTrue(actual.isEmpty)
    }

    @Test
    @DisplayName("Should return the last sync job")
    fun testGetLastSyncJobForConnectionId() {
      val jobId1 = jobPersistence.enqueueJob(SCOPE, SYNC_JOB_CONFIG, true).orElseThrow()
      jobPersistence.succeedAttempt(jobId1, jobPersistence.createAttempt(jobId1, LOG_PATH))

      val afterNow: Instant = NOW.plusSeconds(1000)
      every { timeSupplier.get() } returns afterNow
      val jobId2 = jobPersistence.enqueueJob(SCOPE, SYNC_JOB_CONFIG, true).orElseThrow()

      val actual = jobPersistence.getLastReplicationJob(CONNECTION_ID)
      val expected = createJob(jobId2, SYNC_JOB_CONFIG, JobStatus.PENDING, emptyList(), afterNow.epochSecond)

      assertEquals(Optional.of(expected), actual)
    }

    @Test
    @DisplayName("Should return the last reset job")
    fun testGetLastResetJobForConnectionId() {
      val jobId1 = jobPersistence.enqueueJob(SCOPE, RESET_JOB_CONFIG, true).orElseThrow()
      jobPersistence.succeedAttempt(jobId1, jobPersistence.createAttempt(jobId1, LOG_PATH))

      val afterNow: Instant = NOW.plusSeconds(1000)
      every { timeSupplier.get() } returns afterNow
      val jobId2 = jobPersistence.enqueueJob(SCOPE, RESET_JOB_CONFIG, true).orElseThrow()

      val actual = jobPersistence.getLastReplicationJob(CONNECTION_ID)
      val expected = createJob(jobId2, RESET_JOB_CONFIG, JobStatus.PENDING, emptyList(), afterNow.epochSecond)

      assertEquals(Optional.of(expected), actual)
    }
  }

  @Nested
  @DisplayName("When getting last sync job")
  internal inner class GetLastSyncJob {
    @Test
    @DisplayName("Should return nothing if no job exists")
    fun testGetLastSyncJobForConnectionIdEmpty() {
      val actual = jobPersistence.getLastSyncJob(CONNECTION_ID)

      assertTrue(actual.isEmpty)
    }

    @Test
    @DisplayName("Should return the last enqueued sync job")
    fun testGetLastSyncJobForConnectionId() {
      val jobId1 = jobPersistence.enqueueJob(SCOPE, SYNC_JOB_CONFIG, true).orElseThrow()
      jobPersistence.succeedAttempt(jobId1, jobPersistence.createAttempt(jobId1, LOG_PATH))

      val afterNow: Instant = NOW.plusSeconds(1000)
      every { timeSupplier.get() } returns afterNow
      val jobId2 = jobPersistence.enqueueJob(SCOPE, SYNC_JOB_CONFIG, true).orElseThrow()
      val attemptNumber = jobPersistence.createAttempt(jobId2, LOG_PATH)

      // Return the latest sync job even if failed
      jobPersistence.failAttempt(jobId2, attemptNumber)
      val attempt =
        jobPersistence
          .getJob(jobId2)
          .attempts
          .stream()
          .findFirst()
          .orElseThrow()
      jobPersistence.failJob(jobId2)

      val actual = jobPersistence.getLastSyncJob(CONNECTION_ID)
      val expected = createJob(jobId2, SYNC_JOB_CONFIG, JobStatus.FAILED, listOf(attempt), afterNow.epochSecond)

      assertEquals(Optional.of(expected), actual)
    }

    @Test
    @DisplayName("Should return nothing if only reset job exists")
    fun testGetLastSyncJobForConnectionIdEmptyBecauseOnlyReset() {
      val jobId = jobPersistence.enqueueJob(SCOPE, RESET_JOB_CONFIG, true).orElseThrow()
      jobPersistence.succeedAttempt(jobId, jobPersistence.createAttempt(jobId, LOG_PATH))

      val afterNow: Instant = NOW.plusSeconds(1000)
      every { timeSupplier.get() } returns afterNow

      val actual = jobPersistence.getLastSyncJob(CONNECTION_ID)

      assertTrue(actual.isEmpty)
    }
  }

  @Nested
  @DisplayName("When getting the last sync job for multiple connections")
  internal inner class GetLastSyncJobForConnections {
    @Test
    @DisplayName("Should return nothing if no sync job exists")
    fun testGetLastSyncJobsForConnectionsEmpty() {
      val actual = jobPersistence.getLastSyncJobForConnections(CONNECTION_IDS)

      assertTrue(actual.isEmpty())
    }

    @Test
    @DisplayName("Should return the last enqueued sync job for each connection")
    fun testGetLastSyncJobForConnections() {
      val scope1Job1 = jobPersistence.enqueueJob(SCOPE_1, SYNC_JOB_CONFIG, true).orElseThrow()
      jobPersistence.succeedAttempt(scope1Job1, jobPersistence.createAttempt(scope1Job1, LOG_PATH))

      val scope2Job1 = jobPersistence.enqueueJob(SCOPE_2, SYNC_JOB_CONFIG, true).orElseThrow()
      jobPersistence.succeedAttempt(scope2Job1, jobPersistence.createAttempt(scope2Job1, LOG_PATH))

      jobPersistence.enqueueJob(SCOPE_3, SYNC_JOB_CONFIG, true).orElseThrow()

      val afterNow: Instant = NOW.plusSeconds(1000)
      every { timeSupplier.get() } returns afterNow

      val scope1Job2 = jobPersistence.enqueueJob(SCOPE_1, SYNC_JOB_CONFIG, true).orElseThrow()
      val scope1Job2AttemptNumber = jobPersistence.createAttempt(scope1Job2, LOG_PATH)

      // should return the latest sync job even if failed
      jobPersistence.failAttempt(scope1Job2, scope1Job2AttemptNumber)
      jobPersistence.failJob(scope1Job2)

      // will leave this job running
      val scope2Job2 = jobPersistence.enqueueJob(SCOPE_2, SYNC_JOB_CONFIG, true).orElseThrow()
      jobPersistence.createAttempt(scope2Job2, LOG_PATH)

      val actual = jobPersistence.getLastSyncJobForConnections(CONNECTION_IDS)
      val expected = mutableListOf<JobStatusSummary>()
      expected.add(JobStatusSummary(CONNECTION_ID_1, afterNow.epochSecond, JobStatus.FAILED))
      expected.add(JobStatusSummary(CONNECTION_ID_2, afterNow.epochSecond, JobStatus.RUNNING))
      expected.add(JobStatusSummary(CONNECTION_ID_3, NOW.epochSecond, JobStatus.PENDING))

      assertTrue(expected.size == actual.size && expected.containsAll(actual) && actual.containsAll(expected))
    }

    @Test
    @DisplayName("Should return nothing if only reset job exists")
    fun testGetLastSyncJobsForConnectionsEmptyBecauseOnlyReset() {
      val jobId = jobPersistence.enqueueJob(SCOPE_1, RESET_JOB_CONFIG, true).orElseThrow()
      jobPersistence.succeedAttempt(jobId, jobPersistence.createAttempt(jobId, LOG_PATH))

      val afterNow: Instant = NOW.plusSeconds(1000)
      every { timeSupplier.get() } returns afterNow

      val actual = jobPersistence.getLastSyncJobForConnections(CONNECTION_IDS)

      assertTrue(actual.isEmpty())
    }
  }

  @Nested
  @DisplayName("When getting the last running sync job for multiple connections")
  internal inner class GetRunningSyncJobForConnections {
    @Test
    @DisplayName("Should return nothing if no sync job exists")
    fun testGetRunningSyncJobsForConnectionsEmpty() {
      val actual = jobPersistence.getRunningSyncJobForConnections(CONNECTION_IDS)

      assertTrue(actual.isEmpty())
    }

    @Test
    @DisplayName("Should return the last running sync job for each connection")
    fun testGetRunningSyncJobsForConnections() {
      // succeeded jobs should not be present in the result
      val scope1Job1 = jobPersistence.enqueueJob(SCOPE_1, SYNC_JOB_CONFIG, true).orElseThrow()
      jobPersistence.succeedAttempt(scope1Job1, jobPersistence.createAttempt(scope1Job1, LOG_PATH))

      // fail scope2's first job, but later start a running job that should show up in the result
      val scope2Job1 = jobPersistence.enqueueJob(SCOPE_2, SYNC_JOB_CONFIG, true).orElseThrow()
      val scope2Job1AttemptNumber = jobPersistence.createAttempt(scope2Job1, LOG_PATH)
      jobPersistence.failAttempt(scope2Job1, scope2Job1AttemptNumber)
      jobPersistence.failJob(scope2Job1)

      // pending jobs should be present in the result
      val scope3Job1 = jobPersistence.enqueueJob(SCOPE_3, SYNC_JOB_CONFIG, true).orElseThrow()

      val afterNow: Instant = NOW.plusSeconds(1000)
      every { timeSupplier.get() } returns afterNow

      // create a running job/attempt for scope2
      val scope2Job2 = jobPersistence.enqueueJob(SCOPE_2, SYNC_JOB_CONFIG, true).orElseThrow()
      jobPersistence.createAttempt(scope2Job2, LOG_PATH)
      val scope2Job2attempt =
        jobPersistence
          .getJob(scope2Job2)
          .attempts
          .stream()
          .findFirst()
          .orElseThrow()

      val expected: MutableList<Job?> = ArrayList()
      expected.add(
        createJob(
          scope2Job2,
          SYNC_JOB_CONFIG,
          JobStatus.RUNNING,
          listOf(scope2Job2attempt),
          afterNow.epochSecond,
          SCOPE_2,
          true,
        ),
      )
      expected.add(createJob(scope3Job1, SYNC_JOB_CONFIG, JobStatus.PENDING, emptyList(), NOW.epochSecond, SCOPE_3, true))

      val actual = jobPersistence.getRunningSyncJobForConnections(CONNECTION_IDS)
      assertTrue(expected.size == actual.size && expected.containsAll(actual) && actual.containsAll(expected))
    }

    @Test
    @DisplayName("Should return nothing if only a running reset job exists")
    fun testGetRunningSyncJobsForConnectionsEmptyBecauseOnlyReset() {
      val jobId = jobPersistence.enqueueJob(SCOPE_1, RESET_JOB_CONFIG, true).orElseThrow()
      jobPersistence.createAttempt(jobId, LOG_PATH)

      val afterNow: Instant = NOW.plusSeconds(1000)
      every { timeSupplier.get() } returns afterNow

      val actual = jobPersistence.getRunningSyncJobForConnections(CONNECTION_IDS)

      assertTrue(actual.isEmpty())
    }
  }

  @Nested
  @DisplayName("When getting a running job for a single")
  internal inner class GetRunningJobForConnection {
    @Test
    @DisplayName("Should return nothing if no sync job exists")
    fun testGetRunningSyncJobsForConnectionsEmpty() {
      val actual = jobPersistence.getRunningJobForConnection(CONNECTION_ID_1)

      assertTrue(actual.isEmpty())
    }

    @Test
    @DisplayName("Should return a running sync job for the connection")
    fun testGetRunningJobForConnection() {
      val scope1Job1 = jobPersistence.enqueueJob(SCOPE_1, SYNC_JOB_CONFIG, true).orElseThrow()
      jobPersistence.createAttempt(scope1Job1, LOG_PATH)
      val scope1Job1Attempt =
        jobPersistence
          .getJob(scope1Job1)
          .attempts
          .stream()
          .findFirst()
          .orElseThrow()

      val afterNow: Instant = NOW
      every { timeSupplier.get() } returns afterNow

      val expected: MutableList<Job?> = ArrayList()
      expected.add(
        createJob(
          scope1Job1,
          SYNC_JOB_CONFIG,
          JobStatus.RUNNING,
          listOf(scope1Job1Attempt),
          afterNow.epochSecond,
          SCOPE_1,
          true,
        ),
      )

      val actual = jobPersistence.getRunningJobForConnection(CONNECTION_ID_1)
      assertTrue(expected.size == actual.size && expected.containsAll(actual) && actual.containsAll(expected))
    }

    @Test
    @DisplayName("Should return job if only a running reset job exists")
    fun testGetRunningSyncJobsForConnectionsEmptyBecauseOnlyReset() {
      val jobId = jobPersistence.enqueueJob(SCOPE_1, RESET_JOB_CONFIG, true).orElseThrow()
      jobPersistence.createAttempt(jobId, LOG_PATH)
      val scope1Job1Attempt =
        jobPersistence
          .getJob(jobId)
          .attempts
          .stream()
          .findFirst()
          .orElseThrow()

      val afterNow: Instant = NOW
      every { timeSupplier.get() } returns afterNow

      val expected: MutableList<Job?> = ArrayList()
      expected.add(
        createJob(
          jobId,
          RESET_JOB_CONFIG,
          JobStatus.RUNNING,
          listOf(scope1Job1Attempt),
          afterNow.epochSecond,
          SCOPE_1,
          true,
        ),
      )

      val actual = jobPersistence.getRunningJobForConnection(CONNECTION_ID_1)

      assertTrue(expected.size == actual.size && expected.containsAll(actual) && actual.containsAll(expected))
    }
  }

  @Nested
  @DisplayName("When getting first replication job")
  internal inner class GetFirstReplicationJob {
    @Test
    @DisplayName("Should return nothing if no job exists")
    fun testGetFirstSyncJobForConnectionIdEmpty() {
      val actual = jobPersistence.getFirstReplicationJob(CONNECTION_ID)

      assertTrue(actual.isEmpty)
    }

    @Test
    @DisplayName("Should return the first job")
    fun testGetFirstSyncJobForConnectionId() {
      val jobId1 = jobPersistence.enqueueJob(SCOPE, SYNC_JOB_CONFIG, true).orElseThrow()
      jobPersistence.succeedAttempt(jobId1, jobPersistence.createAttempt(jobId1, LOG_PATH))
      val attemptsWithJobInfo = jobPersistence.getJob(jobId1).attempts
      val attempts = listOf(attemptsWithJobInfo[0])

      val afterNow: Instant = NOW.plusSeconds(1000)
      every { timeSupplier.get() } returns afterNow
      jobPersistence.enqueueJob(SCOPE, SYNC_JOB_CONFIG, true).orElseThrow()

      val actual = jobPersistence.getFirstReplicationJob(CONNECTION_ID)
      val expected = createJob(jobId1, SYNC_JOB_CONFIG, JobStatus.SUCCEEDED, attempts, NOW.epochSecond)

      assertEquals(Optional.of(expected), actual)
    }
  }

  @Nested
  @DisplayName("When getting the count of jobs")
  internal inner class GetJobCount {
    @Test
    @DisplayName("Should return the total job count for all connections in any status")
    fun testGetJobCount() {
      val numJobsToCreate = 10
      val ids: List<Long?> =
        buildList {
          // create jobs for connection 1
          (0..<numJobsToCreate / 2).forEach { _ ->
            val jobId = jobPersistence.enqueueJob(CONNECTION_ID.toString(), SPEC_JOB_CONFIG, true).orElseThrow()
            add(jobId)
          }

          // create jobs for connection 2
          (0..<numJobsToCreate / 2).forEach { _ ->
            val jobId = jobPersistence.enqueueJob(CONNECTION_ID2.toString(), SPEC_JOB_CONFIG, true).orElseThrow()
            add(jobId)
          }
        }

      // fail some jobs
      for (i in 0..2) {
        jobPersistence.failJob(ids[i]!!)
      }

      val actualJobCount =
        jobPersistence.getJobCount(setOf(SPEC_JOB_CONFIG.configType), null, null, null, null, null, null)

      assertEquals(numJobsToCreate.toLong(), actualJobCount)
    }

    @Test
    @DisplayName("Should return the total job count for the connection")
    fun testGetJobCountWithConnectionFilter() {
      val numJobsToCreate = 10
      (0..<numJobsToCreate).forEach { _ ->
        jobPersistence.enqueueJob(CONNECTION_ID.toString(), SPEC_JOB_CONFIG, true)
      }

      val actualJobCount =
        jobPersistence.getJobCount(
          setOf(SPEC_JOB_CONFIG.configType),
          CONNECTION_ID.toString(),
          null,
          null,
          null,
          null,
          null,
        )

      assertEquals(numJobsToCreate.toLong(), actualJobCount)
    }

    @Test
    @DisplayName("Should return the total job count for the connection when filtering by failed jobs only")
    fun testGetJobCountWithFailedJobFilter() {
      val numPendingJobsToCreate = 10
      (0..<numPendingJobsToCreate).forEach { _ ->
        jobPersistence.enqueueJob(SCOPE, CHECK_JOB_CONFIG, true)
      }

      val numFailedJobsToCreate = 5
      (0..<numFailedJobsToCreate).forEach { _ ->
        val jobId = jobPersistence.enqueueJob(SCOPE, CHECK_JOB_CONFIG, true).orElseThrow()
        jobPersistence.failJob(jobId)
      }

      val actualJobCount =
        jobPersistence.getJobCount(
          setOf(CHECK_JOB_CONFIG.configType),
          SCOPE,
          listOf(JobStatus.FAILED),
          null,
          null,
          null,
          null,
        )

      assertEquals(numFailedJobsToCreate.toLong(), actualJobCount)
    }

    @Test
    @DisplayName("Should return the total job count for the connection when filtering by failed and cancelled jobs only")
    fun testGetJobCountWithFailedAndCancelledJobFilter() {
      val jobId1 = jobPersistence.enqueueJob(SCOPE, CHECK_JOB_CONFIG, true).orElseThrow()
      jobPersistence.failJob(jobId1)

      val jobId2 = jobPersistence.enqueueJob(SCOPE, CHECK_JOB_CONFIG, true).orElseThrow()
      jobPersistence.cancelJob(jobId2)

      jobPersistence.enqueueJob(SCOPE, CHECK_JOB_CONFIG, true).orElseThrow()

      val actualJobCount =
        jobPersistence.getJobCount(
          setOf(CHECK_JOB_CONFIG.configType),
          SCOPE,
          listOf(JobStatus.FAILED, JobStatus.CANCELLED),
          null,
          null,
          null,
          null,
        )

      assertEquals(2, actualJobCount)
    }

    @Test
    @DisplayName("Should return the total job count filtering by createdAtStart")
    fun testGetJobCountWithCreatedAtStart() {
      val jobId = jobPersistence.enqueueJob(SCOPE, CHECK_JOB_CONFIG, true).orElseThrow()
      val job = jobPersistence.getJob(jobId)
      val jobCreatedAtSeconds = job.createdAtInSecond

      val oneHourEarlier = OffsetDateTime.ofInstant(Instant.ofEpochSecond((jobCreatedAtSeconds)), ZoneOffset.UTC).minusHours(1)
      val oneHourLater = OffsetDateTime.ofInstant(Instant.ofEpochSecond((jobCreatedAtSeconds)), ZoneOffset.UTC).plusHours(1)

      val numJobsCreatedAtStartOneHourEarlier =
        jobPersistence.getJobCount(setOf(CHECK_JOB_CONFIG.configType), SCOPE, null, oneHourEarlier, null, null, null)
      val numJobsCreatedAtStartOneHourLater =
        jobPersistence.getJobCount(setOf(CHECK_JOB_CONFIG.configType), SCOPE, null, oneHourLater, null, null, null)

      assertEquals(1, numJobsCreatedAtStartOneHourEarlier)
      assertEquals(0, numJobsCreatedAtStartOneHourLater)
    }

    @Test
    @DisplayName("Should return the total job count filtering by createdAtEnd")
    fun testGetJobCountCreatedAtEnd() {
      val jobId = jobPersistence.enqueueJob(SCOPE, CHECK_JOB_CONFIG, true).orElseThrow()
      val job = jobPersistence.getJob(jobId)
      val jobCreatedAtSeconds = job.createdAtInSecond

      val oneHourEarlier = OffsetDateTime.ofInstant(Instant.ofEpochSecond(jobCreatedAtSeconds), ZoneOffset.UTC).minusHours(1)
      val oneHourLater = OffsetDateTime.ofInstant(Instant.ofEpochSecond(jobCreatedAtSeconds), ZoneOffset.UTC).plusHours(1)

      val numJobsCreatedAtEndOneHourEarlier =
        jobPersistence.getJobCount(setOf(CHECK_JOB_CONFIG.configType), SCOPE, null, null, oneHourEarlier, null, null)
      val numJobsCreatedAtEndOneHourLater =
        jobPersistence.getJobCount(setOf(CHECK_JOB_CONFIG.configType), SCOPE, null, null, oneHourLater, null, null)

      assertEquals(0, numJobsCreatedAtEndOneHourEarlier)
      assertEquals(1, numJobsCreatedAtEndOneHourLater)
    }

    @Test
    @DisplayName("Should return the total job count filtering by updatedAtStart")
    fun testGetJobCountWithUpdatedAtStart() {
      val jobId = jobPersistence.enqueueJob(SCOPE, CHECK_JOB_CONFIG, true).orElseThrow()
      val job = jobPersistence.getJob(jobId)
      val jobUpdatedAtSeconds = job.updatedAtInSecond

      val oneHourEarlier = OffsetDateTime.ofInstant(Instant.ofEpochSecond((jobUpdatedAtSeconds)), ZoneOffset.UTC).minusHours(1)
      val oneHourLater = OffsetDateTime.ofInstant(Instant.ofEpochSecond((jobUpdatedAtSeconds)), ZoneOffset.UTC).plusHours(1)

      val numJobsUpdatedAtStartOneHourEarlier =
        jobPersistence.getJobCount(setOf(CHECK_JOB_CONFIG.configType), SCOPE, null, null, null, oneHourEarlier, null)
      val numJobsUpdatedAtStartOneDayLater =
        jobPersistence.getJobCount(setOf(CHECK_JOB_CONFIG.configType), SCOPE, null, null, null, oneHourLater, null)

      assertEquals(1, numJobsUpdatedAtStartOneHourEarlier)
      assertEquals(0, numJobsUpdatedAtStartOneDayLater)
    }

    @Test
    @DisplayName("Should return the total job count filtering by updatedAtEnd")
    fun testGetJobCountUpdatedAtEnd() {
      val jobId = jobPersistence.enqueueJob(SCOPE, CHECK_JOB_CONFIG, true).orElseThrow()
      val job = jobPersistence.getJob(jobId)
      val jobUpdatedAtSeconds = job.updatedAtInSecond

      val oneHourEarlier = OffsetDateTime.ofInstant(Instant.ofEpochSecond((jobUpdatedAtSeconds)), ZoneOffset.UTC).minusHours(1)
      val oneHourLater = OffsetDateTime.ofInstant(Instant.ofEpochSecond((jobUpdatedAtSeconds)), ZoneOffset.UTC).plusHours(1)

      val numJobsUpdatedAtEndOneHourEarlier =
        jobPersistence.getJobCount(setOf(CHECK_JOB_CONFIG.configType), SCOPE, null, null, null, null, oneHourEarlier)
      val numJobsUpdatedAtEndOneHourLater =
        jobPersistence.getJobCount(setOf(CHECK_JOB_CONFIG.configType), SCOPE, null, null, null, null, oneHourLater)

      assertEquals(0, numJobsUpdatedAtEndOneHourEarlier)
      assertEquals(1, numJobsUpdatedAtEndOneHourLater)
    }

    @Test
    @DisplayName("Should return 0 if there are no jobs for this connection")
    fun testGetJobCountNoneForConnection() {
      val otherConnectionId1 = UUID.randomUUID()
      val otherConnectionId2 = UUID.randomUUID()

      jobPersistence.enqueueJob(otherConnectionId1.toString(), SPEC_JOB_CONFIG, true)
      jobPersistence.enqueueJob(otherConnectionId2.toString(), SPEC_JOB_CONFIG, true)

      val actualJobCount =
        jobPersistence.getJobCount(
          setOf(SPEC_JOB_CONFIG.configType),
          CONNECTION_ID.toString(),
          null,
          null,
          null,
          null,
          null,
        )

      assertEquals(0, actualJobCount)
    }
  }

  @Nested
  @DisplayName("When listing jobs, use paged results")
  internal inner class ListJobs {
    @Test
    @DisplayName("Should return the correct page of results with multiple pages of history")
    fun testListJobsByPage() {
      val ids: List<Long?> =
        buildList {
          (0..49).forEach { _ ->
            val jobId = jobPersistence.enqueueJob(CONNECTION_ID.toString(), SPEC_JOB_CONFIG, true).orElseThrow()
            add(jobId)
            // create two attempts per job to verify pagination is applied at the job record level
            val attemptNum1 = jobPersistence.createAttempt(jobId, LOG_PATH)
            jobPersistence.failAttempt(jobId, attemptNum1)
            jobPersistence.createAttempt(jobId, LOG_PATH)

            // also create a job for another connection, to verify the query is properly filtering down to only
            // jobs for the desired connection
            jobPersistence.enqueueJob(CONNECTION_ID2.toString(), SPEC_JOB_CONFIG, true).orElseThrow()
          }
        }

      val pagesize = 10
      val actualList =
        jobPersistence.listJobs(setOf(SPEC_JOB_CONFIG.configType), CONNECTION_ID.toString(), pagesize)
      assertEquals(pagesize, actualList.size)
      assertEquals(ids[ids.size - 1], actualList[0].id)
    }

    @Test
    @DisplayName("Should return the results in the correct sort order")
    fun testListJobsSortsDescending() {
      val ids: List<Long?> =
        buildList {
          (0..99).forEach { _ ->
            // These have strictly the same created_at due to the setup() above, so should come back sorted by
            // id desc instead.
            val jobId = jobPersistence.enqueueJob(CONNECTION_ID.toString(), SPEC_JOB_CONFIG, true).orElseThrow()
            add(jobId)
          }
        }
      val pagesize = 200
      val actualList =
        jobPersistence.listJobs(setOf(SPEC_JOB_CONFIG.configType), CONNECTION_ID.toString(), pagesize)
      for (i in 0..99) {
        assertEquals(ids[ids.size - (i + 1)], actualList[i].id, "Job ids should have been in order but weren't.")
      }
    }

    @Test
    @DisplayName("Should list all jobs")
    fun testListJobs() {
      val jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()

      val actualList =
        jobPersistence.listJobs(setOf(SPEC_JOB_CONFIG.configType), CONNECTION_ID.toString(), 9999)

      val actual = actualList[0]
      val expected = createJob(jobId, SPEC_JOB_CONFIG, JobStatus.PENDING, emptyList(), NOW.epochSecond)

      assertEquals(1, actualList.size)
      assertEquals(expected, actual)
    }

    @Test
    @DisplayName("Should list all jobs matching multiple config types")
    fun testListJobsMultipleConfigTypes() {
      val specJobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()
      val checkJobId = jobPersistence.enqueueJob(SCOPE, CHECK_JOB_CONFIG, true).orElseThrow()
      // add a third config type that is not added in the listJobs request, to verify that it is not
      // included in the results
      jobPersistence.enqueueJob(SCOPE, SYNC_JOB_CONFIG, true).orElseThrow()

      val actualList =
        jobPersistence.listJobs(
          setOf(SPEC_JOB_CONFIG.configType, CHECK_JOB_CONFIG.configType),
          CONNECTION_ID.toString(),
          9999,
        )

      val expectedList =
        listOf(
          createJob(checkJobId, CHECK_JOB_CONFIG, JobStatus.PENDING, emptyList(), NOW.epochSecond),
          createJob(specJobId, SPEC_JOB_CONFIG, JobStatus.PENDING, emptyList(), NOW.epochSecond),
        )

      assertEquals(expectedList, actualList)
    }

    @Test
    @DisplayName("Should list all jobs with all attempts")
    fun testListJobsWithMultipleAttempts() {
      val jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()
      val attemptNumber0 = jobPersistence.createAttempt(jobId, LOG_PATH)

      jobPersistence.failAttempt(jobId, attemptNumber0)

      val secondAttemptLogPath: Path = LOG_PATH.resolve("2")
      val attemptNumber1 = jobPersistence.createAttempt(jobId, secondAttemptLogPath)

      jobPersistence.succeedAttempt(jobId, attemptNumber1)

      val actualList =
        jobPersistence.listJobs(setOf(SPEC_JOB_CONFIG.configType), CONNECTION_ID.toString(), 9999)

      val actual = actualList[0]
      val expected =
        createJob(
          jobId,
          SPEC_JOB_CONFIG,
          JobStatus.SUCCEEDED,
          listOf(
            createAttempt(0, jobId, AttemptStatus.FAILED, LOG_PATH),
            createAttempt(1, jobId, AttemptStatus.SUCCEEDED, secondAttemptLogPath),
          ),
          NOW.epochSecond,
        )

      assertEquals(1, actualList.size)
      assertEquals(expected, actual)
    }

    @Test
    @DisplayName("Should list all jobs light with all attempts")
    fun testListJobsLightWithMultipleAttempts() {
      val jobId = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()
      val attemptNumber0 = jobPersistence.createAttempt(jobId, LOG_PATH)

      jobPersistence.failAttempt(jobId, attemptNumber0)

      val secondAttemptLogPath: Path = LOG_PATH.resolve("2")
      val attemptNumber1 = jobPersistence.createAttempt(jobId, secondAttemptLogPath)

      jobPersistence.succeedAttempt(jobId, attemptNumber1)

      val actualList =
        jobPersistence.listJobsLight(setOf(SPEC_JOB_CONFIG.configType), CONNECTION_ID.toString(), 9999)

      val actual = actualList[0]
      val expected =
        createJob(
          jobId,
          SPEC_JOB_CONFIG,
          JobStatus.SUCCEEDED,
          listOf(
            createAttemptLight(0, jobId, AttemptStatus.FAILED, LOG_PATH),
            createAttemptLight(1, jobId, AttemptStatus.SUCCEEDED, secondAttemptLogPath),
          ),
          NOW.epochSecond,
        )

      assertEquals(1, actualList.size)
      assertEquals(expected, actual)
    }

    @Test
    @DisplayName("Should list all jobs with all attempts in descending order")
    fun testListJobsWithMultipleAttemptsInDescOrder() {
      // create first job with multiple attempts
      val jobId1 = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()
      val job1Attempt1 = jobPersistence.createAttempt(jobId1, LOG_PATH)
      jobPersistence.failAttempt(jobId1, job1Attempt1)
      val job1Attempt2LogPath: Path = LOG_PATH.resolve("2")
      val job1Attempt2 = jobPersistence.createAttempt(jobId1, job1Attempt2LogPath)
      jobPersistence.succeedAttempt(jobId1, job1Attempt2)

      // create second job with multiple attempts
      val laterTime: Instant = NOW.plusSeconds(1000)
      every { timeSupplier.get() } returns laterTime
      val jobId2 = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()
      val job2Attempt1LogPath: Path = LOG_PATH.resolve("3")
      val job2Attempt1 = jobPersistence.createAttempt(jobId2, job2Attempt1LogPath)
      jobPersistence.succeedAttempt(jobId2, job2Attempt1)

      val actualList =
        jobPersistence.listJobs(setOf(SPEC_JOB_CONFIG.configType), CONNECTION_ID.toString(), 9999)

      assertEquals(2, actualList.size)
      assertEquals(jobId2, actualList[0].id)
    }

    @Test
    @DisplayName("Should apply limits after ordering by the key provided by the caller")
    fun testListJobsOrderedByUpdatedAt() {
      val jobId1 = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()
      val job1Attempt1 = jobPersistence.createAttempt(jobId1, LOG_PATH)

      val laterTime: Instant = NOW.plusSeconds(1000)
      every { timeSupplier.get() } returns laterTime
      val jobId2 = jobPersistence.enqueueJob(SCOPE, SPEC_JOB_CONFIG, true).orElseThrow()
      val job2Attempt1LogPath: Path = LOG_PATH.resolve("3")
      val job2Attempt1 = jobPersistence.createAttempt(jobId2, job2Attempt1LogPath)
      jobPersistence.succeedAttempt(jobId2, job2Attempt1)

      val evenLaterTime: Instant = NOW.plusSeconds(3000)
      every { timeSupplier.get() } returns evenLaterTime
      jobPersistence.succeedAttempt(jobId1, job1Attempt1)

      val configId: String? = null
      val updatedAtJobs =
        jobPersistence.listJobs(
          setOf(SPEC_JOB_CONFIG.configType),
          configId,
          1,
          0,
          null,
          null,
          null,
          null,
          null,
          "updatedAt",
          "ASC",
        )
      assertEquals(1, updatedAtJobs.size)
      assertEquals(jobId2, updatedAtJobs[0].id)
      val createdAtJobs =
        jobPersistence.listJobs(
          setOf(SPEC_JOB_CONFIG.configType),
          configId,
          1,
          0,
          null,
          null,
          null,
          null,
          null,
          "createdAt",
          "ASC",
        )
      assertEquals(1, createdAtJobs.size)
      assertEquals(jobId1, createdAtJobs[0].id)
    }

    @Test
    @DisplayName("Should list jobs across all connections in any status")
    fun testListJobsWithNoFilters() {
      val numJobsToCreate = 10
      val ids: List<Long?> =
        buildList {
          (0..<numJobsToCreate / 2).forEach { _ ->
            val connection1JobId = jobPersistence.enqueueJob(CONNECTION_ID.toString(), SPEC_JOB_CONFIG, true).orElseThrow()
            add(connection1JobId)
          }

          (0..<numJobsToCreate / 2).forEach { _ ->
            val connection2JobId = jobPersistence.enqueueJob(CONNECTION_ID2.toString(), SPEC_JOB_CONFIG, true).orElseThrow()
            add(connection2JobId)
          }
        }

      // fail some jobs
      for (i in 0..2) {
        jobPersistence.failJob(ids[i]!!)
      }

      val connectionId: String? = null
      val jobs =
        jobPersistence.listJobs(
          setOf(SPEC_JOB_CONFIG.configType),
          connectionId,
          9999,
          0,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
        )

      assertEquals(HashSet(ids), jobs.stream().map { j: Job? -> j!!.id }.collect(Collectors.toSet()))
    }

    @Test
    @DisplayName("Should list jobs for one connection only")
    fun testListJobsWithConnectionFilters() {
      val numJobsToCreate = 10
      val idsConnection1: Set<Long?> =
        buildSet {
          (0..<numJobsToCreate / 2).forEach { _ ->
            val connection1JobId = jobPersistence.enqueueJob(CONNECTION_ID.toString(), SPEC_JOB_CONFIG, true).orElseThrow()
            add(connection1JobId)
          }
        }

      (0..<numJobsToCreate / 2).forEach { _ ->
        jobPersistence.enqueueJob(CONNECTION_ID2.toString(), SPEC_JOB_CONFIG, true).orElseThrow()
      }

      val jobs =
        jobPersistence.listJobs(
          setOf(SPEC_JOB_CONFIG.configType),
          CONNECTION_ID.toString(),
          9999,
          0,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
        )

      assertEquals(idsConnection1, jobs.stream().map { j: Job? -> j!!.id }.collect(Collectors.toSet()))
    }

    @Test
    @DisplayName("Should list jobs filtering by failed and cancelled jobs")
    fun testListJobWithFailedAndCancelledJobFilter() {
      val jobId1 = jobPersistence.enqueueJob(CONNECTION_ID.toString(), SPEC_JOB_CONFIG, true).orElseThrow()
      jobPersistence.failJob(jobId1)

      val jobId2 = jobPersistence.enqueueJob(CONNECTION_ID.toString(), SPEC_JOB_CONFIG, true).orElseThrow()
      jobPersistence.cancelJob(jobId2)

      val jobId3 = jobPersistence.enqueueJob(CONNECTION_ID.toString(), SPEC_JOB_CONFIG, true).orElseThrow()

      val jobs =
        jobPersistence
          .listJobs(
            setOf(SPEC_JOB_CONFIG.configType),
            CONNECTION_ID.toString(),
            9999,
            0,
            listOf(JobStatus.FAILED, JobStatus.CANCELLED),
            null,
            null,
            null,
            null,
            null,
            null,
          ).toMutableList()

      val actualIds = jobs.stream().map { j: Job? -> j!!.id }.collect(Collectors.toSet())
      assertEquals(2, actualIds.size)
      assertFalse(actualIds.contains(jobId3))
      assertTrue(actualIds.contains(jobId1))
      assertTrue(actualIds.contains(jobId2))
    }

    @Test
    @DisplayName("Should list jobs including the specified job across all connections")
    fun testListJobsIncludingId() {
      val ids: MutableList<Long?> = ArrayList()
      for (i in 0..99) {
        // This makes each enqueued job have an increasingly higher createdAt time
        every { timeSupplier.get() } returns Instant.ofEpochSecond(i.toLong())
        // Alternate between spec and check job config types to verify that both config types are fetched
        // properly
        val jobConfig: JobConfig = if (i % 2 == 0) SPEC_JOB_CONFIG else CHECK_JOB_CONFIG
        // spread across different connections
        val connectionId = if (i % 4 == 0) CONNECTION_ID.toString() else CONNECTION_ID2.toString()
        val jobId = jobPersistence.enqueueJob(connectionId, jobConfig, true).orElseThrow()
        ids.add(jobId)
        // also create an attempt for each job to verify that joining with attempts does not cause failures
        jobPersistence.createAttempt(jobId, LOG_PATH)
      }

      val includingIdIndex = 90
      val pageSize = 25
      val actualList =
        jobPersistence
          .listJobsIncludingId(
            setOf(SPEC_JOB_CONFIG.configType, CHECK_JOB_CONFIG.configType),
            null,
            ids[includingIdIndex]!!,
            pageSize,
          ).toMutableList()
      val expectedJobIds = ids.subList(ids.size - pageSize, ids.size).reversed()
      assertEquals(expectedJobIds, actualList.stream().map { j: Job? -> j!!.id }.toList())
    }

    @Test
    @DisplayName("Should list jobs including the specified job")
    fun testListJobsIncludingIdWithConnectionFilter() {
      val ids: MutableList<Long?> = mutableListOf()
      for (i in 0..99) {
        // This makes each enqueued job have an increasingly higher createdAt time
        every { timeSupplier.get() } returns Instant.ofEpochSecond(i.toLong())
        // Alternate between spec and check job config types to verify that both config types are fetched
        // properly
        val jobConfig: JobConfig = if (i % 2 == 0) SPEC_JOB_CONFIG else CHECK_JOB_CONFIG
        val jobId = jobPersistence.enqueueJob(CONNECTION_ID.toString(), jobConfig, true).orElseThrow()
        ids.add(jobId)
        // also create an attempt for each job to verify that joining with attempts does not cause failures
        jobPersistence.createAttempt(jobId, LOG_PATH)
      }

      val includingIdIndex = 90
      val pageSize = 25
      val actualList =
        jobPersistence
          .listJobsIncludingId(
            setOf(SPEC_JOB_CONFIG.configType, CHECK_JOB_CONFIG.configType),
            CONNECTION_ID.toString(),
            ids[includingIdIndex]!!,
            pageSize,
          ).toMutableList()
      val expectedJobIds = ids.subList(ids.size - pageSize, ids.size).reversed()
      assertEquals(expectedJobIds, actualList.stream().map { j: Job? -> j!!.id }.toList())
    }

    @Test
    @DisplayName("Should list jobs including the specified job, including multiple pages if necessary")
    fun testListJobsIncludingIdMultiplePages() {
      val ids: MutableList<Long?> = ArrayList()
      for (i in 0..99) {
        // This makes each enqueued job have an increasingly higher createdAt time
        every { timeSupplier.get() } returns Instant.ofEpochSecond(i.toLong())
        // Alternate between spec and check job config types to verify that both config types are fetched
        // properly
        val jobConfig: JobConfig = if (i % 2 == 0) SPEC_JOB_CONFIG else CHECK_JOB_CONFIG
        val jobId = jobPersistence.enqueueJob(CONNECTION_ID.toString(), jobConfig, true).orElseThrow()
        ids.add(jobId)
        // also create an attempt for each job to verify that joining with attempts does not cause failures
        jobPersistence.createAttempt(jobId, LOG_PATH)
      }

      // including id is on the second page, so response should contain two pages of jobs
      val includingIdIndex = 60
      val pageSize = 25
      val actualList =
        jobPersistence
          .listJobsIncludingId(
            setOf(SPEC_JOB_CONFIG.configType, CHECK_JOB_CONFIG.configType),
            CONNECTION_ID.toString(),
            ids[includingIdIndex]!!,
            pageSize,
          ).toMutableList()
      val expectedJobIds = ids.subList(ids.size - (pageSize * 2), ids.size).reversed()
      assertEquals(expectedJobIds, actualList.stream().map { j: Job? -> j!!.id }.toList())
    }

    @Test
    @DisplayName("Should return an empty list if there is no job with the includingJob ID for this connection")
    fun testListJobsIncludingIdFromWrongConnection() {
      (0..9).forEach { _ ->
        jobPersistence.enqueueJob(CONNECTION_ID.toString(), SPEC_JOB_CONFIG, true)
      }

      val otherConnectionJobId = jobPersistence.enqueueJob(UUID.randomUUID().toString(), SPEC_JOB_CONFIG, true).orElseThrow()

      val actualList =
        jobPersistence
          .listJobsIncludingId(
            setOf(SPEC_JOB_CONFIG.configType),
            CONNECTION_ID.toString(),
            otherConnectionJobId,
            25,
          ).toMutableList()
      assertEquals(mutableListOf<Any?>(), actualList)
    }
  }

  @Nested
  @DisplayName("When listing job with status")
  internal inner class ListJobsWithStatus {
    @Test
    @DisplayName("Should only list jobs for the requested connection and with the requested statuses and config types")
    fun testListJobsWithStatusesAndConfigTypesForConnection() {
      val desiredConnectionId = UUID.randomUUID()
      val otherConnectionId = UUID.randomUUID()

      // desired connection, statuses, and config types
      val desiredJobId1 = jobPersistence.enqueueJob(desiredConnectionId.toString(), SYNC_JOB_CONFIG, true).orElseThrow()
      jobPersistence.succeedAttempt(desiredJobId1, jobPersistence.createAttempt(desiredJobId1, LOG_PATH))
      val desiredJobId2 = jobPersistence.enqueueJob(desiredConnectionId.toString(), SYNC_JOB_CONFIG, true).orElseThrow()
      val desiredJobId3 = jobPersistence.enqueueJob(desiredConnectionId.toString(), CHECK_JOB_CONFIG, true).orElseThrow()
      jobPersistence.succeedAttempt(desiredJobId3, jobPersistence.createAttempt(desiredJobId3, LOG_PATH))
      val desiredJobId4 = jobPersistence.enqueueJob(desiredConnectionId.toString(), CHECK_JOB_CONFIG, true).orElseThrow()

      // right connection id and status, wrong config type
      jobPersistence.enqueueJob(desiredConnectionId.toString(), SPEC_JOB_CONFIG, true).orElseThrow()
      // right config type and status, wrong connection id
      jobPersistence.enqueueJob(otherConnectionId.toString(), SYNC_JOB_CONFIG, true).orElseThrow()
      // right connection id and config type, wrong status
      val otherJobId3 = jobPersistence.enqueueJob(desiredConnectionId.toString(), CHECK_JOB_CONFIG, true).orElseThrow()
      jobPersistence.failAttempt(otherJobId3, jobPersistence.createAttempt(otherJobId3, LOG_PATH))

      val actualJobs =
        jobPersistence
          .listJobsForConnectionWithStatuses(
            desiredConnectionId,
            setOf(ConfigType.SYNC, ConfigType.CHECK_CONNECTION_DESTINATION),
            setOf(JobStatus.PENDING, JobStatus.SUCCEEDED),
          ).toMutableList()

      val expectedDesiredJob1: Job =
        createJob(
          desiredJobId1,
          SYNC_JOB_CONFIG,
          JobStatus.SUCCEEDED,
          listOf(createAttempt(0, desiredJobId1, AttemptStatus.SUCCEEDED, LOG_PATH)),
          NOW.epochSecond,
          desiredConnectionId.toString(),
          true,
        )
      val expectedDesiredJob2: Job =
        createJob(
          desiredJobId2,
          SYNC_JOB_CONFIG,
          JobStatus.PENDING,
          emptyList(),
          NOW.epochSecond,
          desiredConnectionId.toString(),
          true,
        )
      val expectedDesiredJob3: Job =
        createJob(
          desiredJobId3,
          CHECK_JOB_CONFIG,
          JobStatus.SUCCEEDED,
          listOf(createAttempt(0, desiredJobId3, AttemptStatus.SUCCEEDED, LOG_PATH)),
          NOW.epochSecond,
          desiredConnectionId.toString(),
          true,
        )
      val expectedDesiredJob4: Job =
        createJob(
          desiredJobId4,
          CHECK_JOB_CONFIG,
          JobStatus.PENDING,
          emptyList(),
          NOW.epochSecond,
          desiredConnectionId.toString(),
          true,
        )

      assertEquals(
        setOf(expectedDesiredJob1, expectedDesiredJob2, expectedDesiredJob3, expectedDesiredJob4),
        actualJobs.toSet(),
      )
    }
  }

  companion object {
    private val NOW: Instant = Instant.now()
    private val LOG_PATH: Path = Path.of("/tmp/logs/all/the/way/down")
    private val CONNECTION_ID: UUID = UUID.randomUUID()
    private val CONNECTION_ID_1: UUID = UUID.randomUUID()
    private val CONNECTION_ID_2: UUID = UUID.randomUUID()
    private val CONNECTION_ID_3: UUID = UUID.randomUUID()
    private val SCOPE = CONNECTION_ID.toString()
    private val SCOPE_1 = CONNECTION_ID_1.toString()
    private val SCOPE_2 = CONNECTION_ID_2.toString()
    private val SCOPE_3 = CONNECTION_ID_3.toString()
    private val CONNECTION_IDS = listOf(CONNECTION_ID_1, CONNECTION_ID_2, CONNECTION_ID_3)
    private val CONNECTION_ID2: UUID = UUID.randomUUID()
    private val SPEC_JOB_CONFIG: JobConfig =
      JobConfig()
        .withConfigType(ConfigType.GET_SPEC)
        .withGetSpec(JobGetSpecConfig())

    private val CHECK_JOB_CONFIG: JobConfig =
      JobConfig()
        .withConfigType(ConfigType.CHECK_CONNECTION_DESTINATION)
        .withGetSpec(JobGetSpecConfig())
    private val SYNC_JOB_CONFIG: JobConfig =
      JobConfig()
        .withConfigType(ConfigType.SYNC)
        .withSync(JobSyncConfig())

    private val RESET_JOB_CONFIG: JobConfig =
      JobConfig()
        .withConfigType(ConfigType.RESET_CONNECTION)
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

    private fun createAttempt(
      id: Int,
      jobId: Long,
      status: AttemptStatus,
      logPath: Path?,
    ): Attempt =
      Attempt(
        id,
        jobId,
        logPath,
        null,
        null,
        status,
        null,
        null,
        NOW.epochSecond,
        NOW.epochSecond,
        NOW.epochSecond,
      )

    private fun createAttemptLight(
      id: Int,
      jobId: Long,
      status: AttemptStatus,
      logPath: Path?,
    ): Attempt =
      Attempt(
        id,
        jobId,
        logPath,
        AttemptSyncConfig(),
        JobOutput(),
        status,
        null,
        null,
        NOW.epochSecond,
        NOW.epochSecond,
        NOW.epochSecond,
      )

    private fun createUnfinishedAttempt(
      id: Int,
      jobId: Long,
      status: AttemptStatus,
      logPath: Path?,
    ): Attempt =
      Attempt(
        id,
        jobId,
        logPath,
        null,
        null,
        status,
        null,
        null,
        NOW.epochSecond,
        NOW.epochSecond,
        null,
      )

    private fun createJob(
      id: Long,
      jobConfig: JobConfig,
      status: JobStatus,
      attempts: List<Attempt>,
      time: Long,
      isScheduled: Boolean = true,
    ): Job = createJob(id, jobConfig, status, attempts, time, SCOPE, isScheduled)

    private fun createJob(
      id: Long,
      jobConfig: JobConfig,
      status: JobStatus,
      attempts: List<Attempt>,
      time: Long,
      scope: String,
      isScheduled: Boolean,
    ): Job =
      Job(
        id,
        jobConfig.configType,
        scope,
        jobConfig,
        attempts,
        status,
        null,
        time,
        time,
        isScheduled,
      )

    private fun incrementingSecondSupplier(startTime: Instant): Supplier<Instant> {
      // needs to be an array to work with lambda
      val intArray = intArrayOf(0)

      val timeSupplier = Supplier { startTime.plusSeconds((intArray[0]++).toLong()) }
      return timeSupplier
    }
  }
}
