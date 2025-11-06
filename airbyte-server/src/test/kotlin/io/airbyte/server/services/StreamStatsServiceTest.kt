/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.services

import io.airbyte.commons.json.Jsons
import io.airbyte.config.AirbyteStream
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.JobStatus
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.SyncMode
import io.airbyte.data.repositories.StreamStatsRepository
import io.airbyte.data.repositories.entities.StreamStats
import io.airbyte.persistence.job.JobPersistence
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class StreamStatsServiceTest {
  private val streamStatsRepository = mockk<StreamStatsRepository>()
  private val jobPersistence = mockk<JobPersistence>()
  private val service = StreamStatsService(streamStatsRepository, jobPersistence)

  companion object {
    private const val JOB_ID = 1L
    private const val ATTEMPT_1 = 10L
    private const val ATTEMPT_2 = 11L
    private const val STREAM_NAME_1 = "users"
    private const val STREAM_NAME_2 = "orders"
    private const val NAMESPACE = "public"
    private val CONNECTION_ID = UUID.randomUUID()

    private const val METRIC_1 = "metric1"
    private const val METRIC_2 = "metric2"
    private const val METRIC_3 = "metric3"
  }

  @Test
  fun `test incremental aggregation sums all attempts`() {
    // Setup: Incremental stream with 2 attempts
    val stream1Stats1 =
      StreamStats(
        id = UUID.randomUUID(),
        attemptId = ATTEMPT_1,
        streamName = STREAM_NAME_1,
        streamNamespace = NAMESPACE,
        recordsEmitted = 100,
        bytesEmitted = 1000,
        recordsCommitted = 90,
        bytesCommitted = 900,
        recordsRejected = 10,
        connectionId = CONNECTION_ID,
        additionalStats = mapOf(METRIC_1 to 10.0.toBigDecimal(), METRIC_2 to 20.0.toBigDecimal()),
        wasBackfilled = false,
        wasResumed = false,
      )

    val stream1Stats2 =
      StreamStats(
        id = UUID.randomUUID(),
        attemptId = ATTEMPT_2,
        streamName = STREAM_NAME_1,
        streamNamespace = NAMESPACE,
        recordsEmitted = 200,
        bytesEmitted = 2000,
        recordsCommitted = 180,
        bytesCommitted = 1800,
        recordsRejected = 20,
        connectionId = CONNECTION_ID,
        additionalStats = mapOf(METRIC_1 to 30.0.toBigDecimal(), METRIC_2 to 40.0.toBigDecimal()),
        wasBackfilled = false,
        wasResumed = false,
      )

    val job = createJobWithStreams(listOf(STREAM_NAME_1 to SyncMode.INCREMENTAL))

    every { jobPersistence.getJob(JOB_ID) } returns job
    every { streamStatsRepository.findByJobId(JOB_ID) } returns listOf(stream1Stats1, stream1Stats2)

    // Execute
    val result = service.getAggregatedStatsForJob(JOB_ID)

    // Verify - incremental should sum all attempts
    assertEquals(1, result.size)
    val aggregated = result.first()
    assertEquals(STREAM_NAME_1, aggregated.streamName)
    assertEquals(NAMESPACE, aggregated.streamNamespace)
    assertEquals(300, aggregated.recordsEmitted) // 100 + 200
    assertEquals(3000, aggregated.bytesEmitted) // 1000 + 2000
    assertEquals(270, aggregated.recordsCommitted) // 90 + 180
    assertEquals(2700, aggregated.bytesCommitted) // 900 + 1800
    assertEquals(30, aggregated.recordsRejected) // 10 + 20
    assertEquals(40.0.toBigDecimal(), aggregated.additionalStats[METRIC_1]) // 10 + 30
    assertEquals(60.0.toBigDecimal(), aggregated.additionalStats[METRIC_2]) // 20 + 40
    assertEquals(false, aggregated.wasBackfilled)
    assertEquals(false, aggregated.wasResumed)
  }

  @Test
  fun `test full refresh without resume uses only last attempt`() {
    // Setup: Full refresh stream with 2 attempts, no resume
    val stream1Stats1 =
      StreamStats(
        id = UUID.randomUUID(),
        attemptId = ATTEMPT_1,
        streamName = STREAM_NAME_1,
        streamNamespace = NAMESPACE,
        recordsEmitted = 100,
        bytesEmitted = 1000,
        recordsCommitted = 90,
        bytesCommitted = 900,
        recordsRejected = 10,
        connectionId = CONNECTION_ID,
        additionalStats = mapOf(METRIC_1 to 10.0.toBigDecimal()),
        wasBackfilled = false,
        wasResumed = false,
      )

    val stream1Stats2 =
      StreamStats(
        id = UUID.randomUUID(),
        attemptId = ATTEMPT_2,
        streamName = STREAM_NAME_1,
        streamNamespace = NAMESPACE,
        recordsEmitted = 200,
        bytesEmitted = 2000,
        recordsCommitted = 180,
        bytesCommitted = 1800,
        recordsRejected = 20,
        connectionId = CONNECTION_ID,
        additionalStats = mapOf(METRIC_1 to 30.0.toBigDecimal()),
        wasBackfilled = false,
        wasResumed = false,
      )

    val job = createJobWithStreams(listOf(STREAM_NAME_1 to SyncMode.FULL_REFRESH))

    every { jobPersistence.getJob(JOB_ID) } returns job
    every { streamStatsRepository.findByJobId(JOB_ID) } returns listOf(stream1Stats1, stream1Stats2)

    // Execute
    val result = service.getAggregatedStatsForJob(JOB_ID)

    // Verify - full refresh without resume should use only the last attempt
    assertEquals(1, result.size)
    val aggregated = result.first()
    assertEquals(200, aggregated.recordsEmitted) // Only attempt 2
    assertEquals(2000, aggregated.bytesEmitted)
    assertEquals(180, aggregated.recordsCommitted)
    assertEquals(1800, aggregated.bytesCommitted)
    assertEquals(20, aggregated.recordsRejected)
    assertEquals(30.0.toBigDecimal(), aggregated.additionalStats[METRIC_1]) // Only attempt 2
  }

  @Test
  fun `test full refresh with resume sums resumed attempts`() {
    // Setup: Full refresh stream with 3 attempts, last 2 are resumed
    val stream1Stats1 =
      StreamStats(
        id = UUID.randomUUID(),
        attemptId = ATTEMPT_1,
        streamName = STREAM_NAME_1,
        streamNamespace = NAMESPACE,
        recordsEmitted = 100,
        bytesEmitted = 1000,
        recordsCommitted = 90,
        bytesCommitted = 900,
        recordsRejected = 10,
        connectionId = CONNECTION_ID,
        additionalStats = mapOf(METRIC_1 to 10.0.toBigDecimal()),
        wasBackfilled = false,
        wasResumed = false,
      )

    val stream1Stats2 =
      StreamStats(
        id = UUID.randomUUID(),
        attemptId = ATTEMPT_2,
        streamName = STREAM_NAME_1,
        streamNamespace = NAMESPACE,
        recordsEmitted = 50,
        bytesEmitted = 500,
        recordsCommitted = 45,
        bytesCommitted = 450,
        recordsRejected = 5,
        connectionId = CONNECTION_ID,
        additionalStats = mapOf(METRIC_1 to 5.0.toBigDecimal()),
        wasBackfilled = false,
        wasResumed = true,
      )

    val stream1Stats3 =
      StreamStats(
        id = UUID.randomUUID(),
        attemptId = ATTEMPT_2 + 1,
        streamName = STREAM_NAME_1,
        streamNamespace = NAMESPACE,
        recordsEmitted = 150,
        bytesEmitted = 1500,
        recordsCommitted = 135,
        bytesCommitted = 1350,
        recordsRejected = 15,
        connectionId = CONNECTION_ID,
        additionalStats = mapOf(METRIC_1 to 15.0.toBigDecimal()),
        wasBackfilled = false,
        wasResumed = true,
      )

    val job = createJobWithStreams(listOf(STREAM_NAME_1 to SyncMode.FULL_REFRESH))

    every { jobPersistence.getJob(JOB_ID) } returns job
    every { streamStatsRepository.findByJobId(JOB_ID) } returns listOf(stream1Stats1, stream1Stats2, stream1Stats3)

    // Execute
    val result = service.getAggregatedStatsForJob(JOB_ID)

    // Verify - should sum from last non-resumed (attempt 1) + resumed attempts (2 & 3)
    assertEquals(1, result.size)
    val aggregated = result.first()
    assertEquals(300, aggregated.recordsEmitted) // 100 + 50 + 150
    assertEquals(3000, aggregated.bytesEmitted) // 1000 + 500 + 1500
    assertEquals(270, aggregated.recordsCommitted) // 90 + 45 + 135
    assertEquals(2700, aggregated.bytesCommitted) // 900 + 450 + 1350
    assertEquals(30, aggregated.recordsRejected) // 10 + 5 + 15
    assertEquals(30.0.toBigDecimal(), aggregated.additionalStats[METRIC_1]) // 10 + 5 + 15
    assertEquals(true, aggregated.wasResumed) // At least one attempt was resumed
  }

  @Test
  fun `test multiple streams with different sync modes`() {
    // Setup: Stream 1 is incremental, Stream 2 is full refresh
    val stream1Stats1 =
      StreamStats(
        id = UUID.randomUUID(),
        attemptId = ATTEMPT_1,
        streamName = STREAM_NAME_1,
        streamNamespace = NAMESPACE,
        recordsEmitted = 100,
        bytesEmitted = 1000,
        recordsCommitted = 100,
        bytesCommitted = 1000,
        recordsRejected = 0,
        connectionId = CONNECTION_ID,
        additionalStats = mapOf(METRIC_1 to 10.0.toBigDecimal()),
        wasBackfilled = false,
        wasResumed = false,
      )

    val stream1Stats2 =
      StreamStats(
        id = UUID.randomUUID(),
        attemptId = ATTEMPT_2,
        streamName = STREAM_NAME_1,
        streamNamespace = NAMESPACE,
        recordsEmitted = 50,
        bytesEmitted = 500,
        recordsCommitted = 50,
        bytesCommitted = 500,
        recordsRejected = 0,
        connectionId = CONNECTION_ID,
        additionalStats = mapOf(METRIC_1 to 5.0.toBigDecimal()),
        wasBackfilled = false,
        wasResumed = false,
      )

    val stream2Stats1 =
      StreamStats(
        id = UUID.randomUUID(),
        attemptId = ATTEMPT_1,
        streamName = STREAM_NAME_2,
        streamNamespace = NAMESPACE,
        recordsEmitted = 200,
        bytesEmitted = 2000,
        recordsCommitted = 200,
        bytesCommitted = 2000,
        recordsRejected = 0,
        connectionId = CONNECTION_ID,
        additionalStats = mapOf(METRIC_2 to 20.0.toBigDecimal()),
        wasBackfilled = true,
        wasResumed = false,
      )

    val stream2Stats2 =
      StreamStats(
        id = UUID.randomUUID(),
        attemptId = ATTEMPT_2,
        streamName = STREAM_NAME_2,
        streamNamespace = NAMESPACE,
        recordsEmitted = 300,
        bytesEmitted = 3000,
        recordsCommitted = 300,
        bytesCommitted = 3000,
        recordsRejected = 0,
        connectionId = CONNECTION_ID,
        additionalStats = mapOf(METRIC_2 to 30.0.toBigDecimal()),
        wasBackfilled = false,
        wasResumed = false,
      )

    val job =
      createJobWithStreams(
        listOf(
          STREAM_NAME_1 to SyncMode.INCREMENTAL,
          STREAM_NAME_2 to SyncMode.FULL_REFRESH,
        ),
      )

    every { jobPersistence.getJob(JOB_ID) } returns job
    every { streamStatsRepository.findByJobId(JOB_ID) } returns
      listOf(stream1Stats1, stream1Stats2, stream2Stats1, stream2Stats2)

    // Execute
    val result = service.getAggregatedStatsForJob(JOB_ID)

    // Verify
    assertEquals(2, result.size)

    // Stream 1 (incremental) - sum all attempts
    val stream1 = result.find { it.streamName == STREAM_NAME_1 }!!
    assertEquals(150, stream1.recordsEmitted) // 100 + 50
    assertEquals(1500, stream1.bytesEmitted) // 1000 + 500
    assertEquals(15.0.toBigDecimal(), stream1.additionalStats[METRIC_1]) // 10 + 5
    assertEquals(false, stream1.wasBackfilled)

    // Stream 2 (full refresh, no resume) - only last attempt
    val stream2 = result.find { it.streamName == STREAM_NAME_2 }!!
    assertEquals(300, stream2.recordsEmitted) // Only attempt 2
    assertEquals(3000, stream2.bytesEmitted)
    assertEquals(30.0.toBigDecimal(), stream2.additionalStats[METRIC_2]) // Only attempt 2
    assertEquals(true, stream2.wasBackfilled) // At least one attempt was backfilled
  }

  @Test
  fun `test additional stats with different keys across attempts`() {
    // Setup: Attempts have different metric keys
    val stream1Stats1 =
      StreamStats(
        id = UUID.randomUUID(),
        attemptId = ATTEMPT_1,
        streamName = STREAM_NAME_1,
        streamNamespace = NAMESPACE,
        recordsEmitted = 100,
        bytesEmitted = 1000,
        recordsCommitted = 100,
        bytesCommitted = 1000,
        recordsRejected = 0,
        connectionId = CONNECTION_ID,
        additionalStats = mapOf(METRIC_1 to 10.0.toBigDecimal(), METRIC_2 to 20.0.toBigDecimal()),
        wasBackfilled = false,
        wasResumed = false,
      )

    val stream1Stats2 =
      StreamStats(
        id = UUID.randomUUID(),
        attemptId = ATTEMPT_2,
        streamName = STREAM_NAME_1,
        streamNamespace = NAMESPACE,
        recordsEmitted = 200,
        bytesEmitted = 2000,
        recordsCommitted = 200,
        bytesCommitted = 2000,
        recordsRejected = 0,
        connectionId = CONNECTION_ID,
        additionalStats = mapOf(METRIC_2 to 30.0.toBigDecimal(), METRIC_3 to 40.0.toBigDecimal()),
        wasBackfilled = false,
        wasResumed = false,
      )

    val job = createJobWithStreams(listOf(STREAM_NAME_1 to SyncMode.INCREMENTAL))

    every { jobPersistence.getJob(JOB_ID) } returns job
    every { streamStatsRepository.findByJobId(JOB_ID) } returns listOf(stream1Stats1, stream1Stats2)

    // Execute
    val result = service.getAggregatedStatsForJob(JOB_ID)

    // Verify - should have union of all keys
    assertEquals(1, result.size)
    val aggregated = result.first()
    assertEquals(10.0.toBigDecimal(), aggregated.additionalStats[METRIC_1]) // Only in attempt 1
    assertEquals(50.0.toBigDecimal(), aggregated.additionalStats[METRIC_2]) // 20 + 30
    assertEquals(40.0.toBigDecimal(), aggregated.additionalStats[METRIC_3]) // Only in attempt 2
  }

  @Test
  fun `test null additional stats are handled gracefully`() {
    // Setup: One attempt has null additionalStats
    val stream1Stats1 =
      StreamStats(
        id = UUID.randomUUID(),
        attemptId = ATTEMPT_1,
        streamName = STREAM_NAME_1,
        streamNamespace = NAMESPACE,
        recordsEmitted = 100,
        bytesEmitted = 1000,
        recordsCommitted = 100,
        bytesCommitted = 1000,
        recordsRejected = 0,
        connectionId = CONNECTION_ID,
        additionalStats = null,
        wasBackfilled = false,
        wasResumed = false,
      )

    val stream1Stats2 =
      StreamStats(
        id = UUID.randomUUID(),
        attemptId = ATTEMPT_2,
        streamName = STREAM_NAME_1,
        streamNamespace = NAMESPACE,
        recordsEmitted = 200,
        bytesEmitted = 2000,
        recordsCommitted = 200,
        bytesCommitted = 2000,
        recordsRejected = 0,
        connectionId = CONNECTION_ID,
        additionalStats = mapOf(METRIC_1 to 30.0.toBigDecimal()),
        wasBackfilled = false,
        wasResumed = false,
      )

    val job = createJobWithStreams(listOf(STREAM_NAME_1 to SyncMode.INCREMENTAL))

    every { jobPersistence.getJob(JOB_ID) } returns job
    every { streamStatsRepository.findByJobId(JOB_ID) } returns listOf(stream1Stats1, stream1Stats2)

    // Execute
    val result = service.getAggregatedStatsForJob(JOB_ID)

    // Verify - null is treated as empty map
    assertEquals(1, result.size)
    val aggregated = result.first()
    assertEquals(30.0.toBigDecimal(), aggregated.additionalStats[METRIC_1])
  }

  @Test
  fun `test wasBackfilled and wasResumed flags are ORed`() {
    // Setup: Multiple attempts with different flag values
    val stream1Stats1 =
      StreamStats(
        id = UUID.randomUUID(),
        attemptId = ATTEMPT_1,
        streamName = STREAM_NAME_1,
        streamNamespace = NAMESPACE,
        recordsEmitted = 100,
        bytesEmitted = 1000,
        recordsCommitted = 100,
        bytesCommitted = 1000,
        recordsRejected = 0,
        connectionId = CONNECTION_ID,
        additionalStats = mapOf(METRIC_1 to 10.0.toBigDecimal()),
        wasBackfilled = true,
        wasResumed = false,
      )

    val stream1Stats2 =
      StreamStats(
        id = UUID.randomUUID(),
        attemptId = ATTEMPT_2,
        streamName = STREAM_NAME_1,
        streamNamespace = NAMESPACE,
        recordsEmitted = 200,
        bytesEmitted = 2000,
        recordsCommitted = 200,
        bytesCommitted = 2000,
        recordsRejected = 0,
        connectionId = CONNECTION_ID,
        additionalStats = mapOf(METRIC_1 to 30.0.toBigDecimal()),
        wasBackfilled = false,
        wasResumed = true,
      )

    val job = createJobWithStreams(listOf(STREAM_NAME_1 to SyncMode.INCREMENTAL))

    every { jobPersistence.getJob(JOB_ID) } returns job
    every { streamStatsRepository.findByJobId(JOB_ID) } returns listOf(stream1Stats1, stream1Stats2)

    // Execute
    val result = service.getAggregatedStatsForJob(JOB_ID)

    // Verify - flags should be ORed across all attempts
    assertEquals(1, result.size)
    val aggregated = result.first()
    assertEquals(true, aggregated.wasBackfilled) // true OR false = true
    assertEquals(true, aggregated.wasResumed) // false OR true = true
  }

  private fun createJobWithStreams(streams: List<Pair<String, SyncMode>>): Job {
    val configuredStreams =
      streams.map { (name, syncMode) ->
        val airbyteStream =
          AirbyteStream(
            name = name,
            jsonSchema = Jsons.emptyObject(),
            supportedSyncModes = listOf(syncMode),
          ).withNamespace(NAMESPACE)

        ConfiguredAirbyteStream(
          stream = airbyteStream,
        ).withSyncMode(syncMode)
      }

    val catalog = ConfiguredAirbyteCatalog().withStreams(configuredStreams)
    val syncConfig = JobSyncConfig().withConfiguredAirbyteCatalog(catalog)
    val jobConfig =
      JobConfig()
        .withConfigType(JobConfig.ConfigType.SYNC)
        .withSync(syncConfig)

    return Job(
      id = JOB_ID,
      configType = JobConfig.ConfigType.SYNC,
      scope = CONNECTION_ID.toString(),
      config = jobConfig,
      attempts = listOf(),
      status = JobStatus.SUCCEEDED,
      startedAtInSecond = null,
      createdAtInSecond = 0,
      updatedAtInSecond = 0,
      isScheduled = false,
    )
  }
}
