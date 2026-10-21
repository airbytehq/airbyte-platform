/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.server.generated.models.JobAggregatedStats
import io.airbyte.api.server.generated.models.JobConfigType
import io.airbyte.api.server.generated.models.JobInfoRead
import io.airbyte.api.server.generated.models.JobRead
import io.airbyte.api.server.generated.models.JobStatus
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import java.util.UUID

class JobResponseMapperTest {
  @ParameterizedTest
  @EnumSource(value = JobConfigType::class, names = ["SYNC", "RESET_CONNECTION", "REFRESH", "CLEAR"])
  fun `from should convert a JobRead object from the config api to a JobResponse`(jobConfigType: JobConfigType) {
    // Given
    val jobRead = getJobRead(jobConfigType)
    val jobInfoRead = JobInfoRead(job = jobRead, attempts = emptyList())

    // When
    val jobResponse = JobResponseMapper.from(jobInfoRead)

    // Then
    assertEquals(jobResponse.jobId, jobRead.id)
    assertEquals(jobResponse.status.toString(), jobRead.status.toString())
    assertEquals(jobResponse.connectionId, jobRead.configId)
    assertEquals(jobResponse.jobType.toString(), if (jobConfigType == JobConfigType.RESET_CONNECTION) "reset" else jobRead.configType.toString())
    assertEquals(jobResponse.startTime, "1970-01-01T00:00:01Z")
    assertEquals(jobResponse.lastUpdatedAt, "1970-01-01T00:00:02Z")
    assertEquals(jobResponse.duration, "PT1S")
    assertEquals(jobResponse.bytesSynced, jobRead.aggregatedStats!!.bytesCommitted)
    assertEquals(jobResponse.rowsSynced, jobRead.aggregatedStats!!.recordsCommitted)
  }

  @ParameterizedTest
  @EnumSource(value = JobConfigType::class, names = ["SYNC", "RESET_CONNECTION", "REFRESH", "CLEAR"], mode = EnumSource.Mode.EXCLUDE)
  fun `from should fail to convert a JobRead object from the config api if the config type is not supported`(jobConfigType: JobConfigType) {
    val jobRead = getJobRead(jobConfigType)
    val jobInfoRead = JobInfoRead(job = jobRead, attempts = emptyList())

    assertThrows<IllegalArgumentException> { JobResponseMapper.from(jobInfoRead) }
  }

  @Test
  fun `from should convert a queued JobRead object from the config api to a JobResponse`() {
    val jobRead = getJobRead(JobConfigType.SYNC).copy(status = JobStatus.QUEUED)
    val jobInfoRead = JobInfoRead(job = jobRead, attempts = emptyList())

    val jobResponse = JobResponseMapper.from(jobInfoRead)

    assertEquals(jobRead.status.toString(), jobResponse.status.toString())
  }

  private fun getJobRead(jobConfigType: JobConfigType): JobRead =
    JobRead(
      id = 1L,
      status = JobStatus.FAILED,
      configId = UUID.randomUUID().toString(),
      configType = jobConfigType,
      createdAt = 1L,
      updatedAt = 2L,
      aggregatedStats =
        JobAggregatedStats(
          recordsEmitted = 0,
          bytesEmitted = 0,
          recordsCommitted = 67890,
          bytesCommitted = 12345,
          recordsRejected = 0,
        ),
    )
}
