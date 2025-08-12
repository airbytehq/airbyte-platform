/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.data.repositories.AbstractConfigRepositoryTest
import io.airbyte.data.repositories.entities.ObsJobsStats
import io.airbyte.data.repositories.entities.ObsStreamStats
import io.airbyte.data.repositories.entities.ObsStreamStatsId
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.util.UUID

class ObsStatsServiceTest : AbstractConfigRepositoryTest() {
  private val orbStatsService = context.getBean(ObsStatsService::class.java)!!

  @Test
  fun `read, write and update of a jobStats`() {
    val jobStats = Fixtures.defaultObsJobsStats.copy(jobId = 2)
    orbStatsService.saveJobsStats(jobStats)

    val actualJobStats = orbStatsService.getJobStats(jobStats.jobId)
    // override createdAt for test consistency, round trip sometimes changes precision.
    assertEquals(jobStats.copy(createdAt = OffsetDateTime.MIN), actualJobStats.copy(createdAt = OffsetDateTime.MIN))

    val updatedStats = jobStats.copy(attemptCount = 3, durationSeconds = 51)
    orbStatsService.saveJobsStats(updatedStats)
    val actualUpdatedJobStats = orbStatsService.getJobStats(jobStats.jobId)
    // override createdAt for test consistency, round trip sometimes changes precision.
    assertEquals(updatedStats.copy(createdAt = OffsetDateTime.MIN), actualUpdatedJobStats.copy(createdAt = OffsetDateTime.MIN))
  }

  @Test
  fun `read, write and update of streamStats`() {
    val stream1 = Fixtures.defaultObsStreamStats
    val stream2 = Fixtures.defaultObsStreamStatsWithoutNamespace
    val stats = listOf(stream1, stream2)

    orbStatsService.saveStreamStats(stats)
    val actualStats = orbStatsService.getJobStreamStats(stream1.id.jobId)
    assertEquals(stats, actualStats)

    val updatedStats =
      listOf(
        stream1.copy(bytesLoaded = 200),
        stream2.copy(recordsRejected = 1),
      )
    val updateResponse = orbStatsService.saveStreamStats(updatedStats)
    assertEquals(updateResponse, updatedStats)
    val actualUpdatedStats = orbStatsService.getJobStreamStats(stream1.id.jobId)
    assertEquals(updatedStats, actualUpdatedStats)
  }

  @Test
  fun `updates should also work when only updating stats for a stream without namespace`() {
    val id = ObsStreamStatsId(jobId = 5, streamNamespace = "ns", streamName = "s")
    val stream1 = Fixtures.defaultObsStreamStats.copy(id = id)
    val stream2 = Fixtures.defaultObsStreamStats.copy(id = id.copy(streamNamespace = null))
    val stats = listOf(stream1, stream2)
    orbStatsService.saveStreamStats(stats)

    val updatedStream2 = stream2.copy(recordsRejected = 5)
    orbStatsService.saveStreamStats(listOf(updatedStream2))

    val actualStats = orbStatsService.getJobStreamStats(id.jobId)
    val expectedStats = listOf(stream1, updatedStream2)
    assertEquals(expectedStats, actualStats)
  }

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
