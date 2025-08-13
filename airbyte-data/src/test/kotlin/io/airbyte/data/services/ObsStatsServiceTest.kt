/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.data.repositories.AbstractConfigRepositoryTest
import io.airbyte.data.repositories.entities.ObsJobsStats
import io.airbyte.data.repositories.entities.ObsStreamStats
import io.airbyte.data.repositories.entities.ObsStreamStatsId
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobConfigType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.OffsetDateTime
import java.util.UUID

class ObsStatsServiceTest : AbstractConfigRepositoryTest() {
  private val obsStatsService = context.getBean(ObsStatsService::class.java)!!

  @Test
  fun `read, write and update of a jobStats`() {
    val jobStats = Fixtures.defaultObsJobsStats.copy(jobId = 2)
    obsStatsService.saveJobsStats(jobStats)

    val actualJobStats = obsStatsService.getJobStats(jobStats.jobId)
    // override createdAt for test consistency, round trip sometimes changes precision.
    assertEquals(jobStats.filterCreatedAtForTest(), actualJobStats.filterCreatedAtForTest())

    val updatedStats = jobStats.copy(attemptCount = 3, durationSeconds = 51)
    obsStatsService.saveJobsStats(updatedStats)
    val actualUpdatedJobStats = obsStatsService.getJobStats(jobStats.jobId)
    // override createdAt for test consistency, round trip sometimes changes precision.
    assertEquals(updatedStats.filterCreatedAtForTest(), actualUpdatedJobStats.filterCreatedAtForTest())
  }

  @Test
  fun `read, write and update of streamStats`() {
    val stream1 = Fixtures.defaultObsStreamStats
    val stream2 = Fixtures.defaultObsStreamStatsWithoutNamespace
    val stats = listOf(stream1, stream2)

    obsStatsService.saveStreamStats(stats)
    val actualStats = obsStatsService.getJobStreamStats(stream1.id.jobId)
    assertEquals(stats, actualStats)

    val updatedStats =
      listOf(
        stream1.copy(bytesLoaded = 200),
        stream2.copy(recordsRejected = 1),
      )
    val updateResponse = obsStatsService.saveStreamStats(updatedStats)
    assertEquals(updateResponse, updatedStats)
    val actualUpdatedStats = obsStatsService.getJobStreamStats(stream1.id.jobId)
    assertEquals(updatedStats, actualUpdatedStats)
  }

  @Test
  fun `updates should also work when only updating stats for a stream without namespace`() {
    val id = ObsStreamStatsId(jobId = 5, streamNamespace = "ns", streamName = "s")
    val stream1 = Fixtures.defaultObsStreamStats.copy(id = id)
    val stream2 = Fixtures.defaultObsStreamStats.copy(id = id.copy(streamNamespace = null))
    val stats = listOf(stream1, stream2)
    obsStatsService.saveStreamStats(stats)

    val updatedStream2 = stream2.copy(recordsRejected = 5)
    obsStatsService.saveStreamStats(listOf(updatedStream2))

    val actualStats = obsStatsService.getJobStreamStats(id.jobId)
    val expectedStats = listOf(stream1, updatedStream2)
    assertEquals(expectedStats, actualStats)
  }

  @Test
  fun `findJobWithPrevious return and job and it's previous ones given the parameters`() {
    val baseJob = Fixtures.defaultObsJobsStats.copy(connectionId = UUID.randomUUID())
    val baseTime = OffsetDateTime.now().minusMinutes(20)
    val jobs =
      (1L..20L)
        .map {
          baseJob.copy(jobId = it, createdAt = baseTime.plusMinutes(it))
        }.toList()

    jobs.forEach { obsStatsService.saveJobsStats(it) }

    // 5 previous job, within a 10min window before jobId:10
    val query1 = obsStatsService.findJobStatsAndPrevious(10, Duration.ofMinutes(10), 5)
    assertEquals(listOf(10L, 9L, 8L, 7L, 6L, 5L), query1.map { it.jobId })

    // 3 previous job, within a 10min window before jobId:10
    val query2 = obsStatsService.findJobStatsAndPrevious(10, Duration.ofMinutes(10), 3)
    assertEquals(listOf(10L, 9L, 8L, 7L), query2.map { it.jobId })

    // 10 previous job, within a 5min window before jobId:15
    val query3 = obsStatsService.findJobStatsAndPrevious(15, Duration.ofMinutes(5), 10)
    assertEquals(listOf(15L, 14L, 13L, 12L, 11L), query3.map { it.jobId })

    // 10 previous job, within a 5min window before jobId:1
    val query4 = obsStatsService.findJobStatsAndPrevious(1, Duration.ofMinutes(5), 10)
    assertEquals(listOf(1L), query4.map { it.jobId })

    // Doesn't crash on a job not found
    val queryNotFound = obsStatsService.findJobStatsAndPrevious(404, Duration.ofMinutes(5), 10)
    assertEquals(emptyList<ObsJobsStats>(), queryNotFound)
  }

  @Test
  fun `findJobsWithPrevious can filter based on job types`() {
    val baseJob = Fixtures.defaultObsJobsStats.copy(connectionId = UUID.randomUUID())
    val sync50 = baseJob.copy(jobId = 50, jobType = "sync", createdAt = OffsetDateTime.now().minusMinutes(5))
    val refresh51 = baseJob.copy(jobId = 51, jobType = "refresh", createdAt = OffsetDateTime.now().minusMinutes(4))
    val clear52 = baseJob.copy(jobId = 52, jobType = "reset_connection", createdAt = OffsetDateTime.now().minusMinutes(3))
    val sync53 = baseJob.copy(jobId = 53, jobType = "sync", createdAt = OffsetDateTime.now().minusMinutes(2))
    val refresh54 = baseJob.copy(jobId = 54, jobType = "refresh", createdAt = OffsetDateTime.now().minusMinutes(1))
    val clear55 = baseJob.copy(jobId = 55, jobType = "reset_connection", createdAt = OffsetDateTime.now())
    val jobs = listOf(sync50, refresh51, clear52, sync53, refresh54, clear55)

    jobs.forEach { obsStatsService.saveJobsStats(it) }
    jobs.forEach {
      obsStatsService.saveStreamStats(
        listOf(
          Fixtures.defaultObsStreamStats.copy(
            id =
              ObsStreamStatsId(
                jobId = it.jobId,
                streamNamespace = "ns",
                streamName = "s",
              ),
          ),
        ),
      )
    }

    val syncs = obsStatsService.findJobStatsAndPrevious(53, Duration.ofHours(1), limit = 100, jobTypes = listOf(JobConfigType.sync))
    assertEquals(listOf(sync53, sync50).filterCreatedAtForTest(), syncs.filterCreatedAtForTest())

    val clears = obsStatsService.findJobStatsAndPrevious(55, Duration.ofHours(1), limit = 100, jobTypes = listOf(JobConfigType.reset_connection))
    assertEquals(listOf(clear55, clear52).filterCreatedAtForTest(), clears.filterCreatedAtForTest())

    val clearAndRefreshes =
      obsStatsService.findJobStatsAndPrevious(
        54,
        Duration.ofHours(1),
        limit = 100,
        jobTypes = listOf(JobConfigType.reset_connection, JobConfigType.refresh),
      )
    assertEquals(listOf(refresh54, clear52, refresh51).filterCreatedAtForTest(), clearAndRefreshes.filterCreatedAtForTest())

    val stats =
      obsStatsService.findJobStreamStatsAndPrevious(
        54,
        Duration.ofHours(1),
        limit = 100,
        jobTypes = listOf(JobConfigType.reset_connection, JobConfigType.refresh),
      )
    assertEquals(setOf(54L, 52L, 51L), stats.map { it.id.jobId }.toSet())
  }

  private fun ObsJobsStats.filterCreatedAtForTest(): ObsJobsStats = copy(createdAt = OffsetDateTime.MIN)

  private fun List<ObsJobsStats>.filterCreatedAtForTest(): List<ObsJobsStats> = map { it.filterCreatedAtForTest() }

  object Fixtures {
    val defaultObsJobsStats =
      ObsJobsStats(
        jobId = 1,
        connectionId = UUID.randomUUID(),
        workspaceId = UUID.randomUUID(),
        organizationId = UUID.randomUUID(),
        sourceId = UUID.randomUUID(),
        sourceDefinitionId = UUID.randomUUID(),
        sourceImageTag = "1.0.0",
        destinationId = UUID.randomUUID(),
        destinationDefinitionId = UUID.randomUUID(),
        destinationImageTag = "1.0.1",
        createdAt = OffsetDateTime.now(),
        jobType = "sync",
        status = "successful",
        attemptCount = 1,
        durationSeconds = 42,
      )

    val defaultObsStreamStats =
      ObsStreamStats(
        id =
          ObsStreamStatsId(
            jobId = 1,
            streamNamespace = "namespace",
            streamName = "stream1",
          ),
        bytesLoaded = 10,
        recordsLoaded = 1,
        recordsRejected = 0,
        wasBackfilled = false,
        wasResumed = false,
      )

    val defaultObsStreamStatsWithoutNamespace =
      ObsStreamStats(
        id =
          ObsStreamStatsId(
            jobId = 1,
            streamNamespace = null,
            streamName = "stream1",
          ),
        bytesLoaded = 10,
        recordsLoaded = 1,
        recordsRejected = 0,
        wasBackfilled = false,
        wasResumed = false,
      )
  }
}
