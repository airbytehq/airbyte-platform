/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.JobAggregatedStats
import io.airbyte.api.model.generated.JobConfigType
import io.airbyte.api.model.generated.JobInfoRead
import io.airbyte.api.model.generated.JobRead
import io.airbyte.api.model.generated.JobStatus
import org.junit.jupiter.api.Assertions.assertEquals
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
    val jobInfoRead = JobInfoRead()
    jobInfoRead.job = jobRead

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
    assertEquals(jobResponse.bytesSynced, jobRead.aggregatedStats.bytesCommitted)
    assertEquals(jobResponse.rowsSynced, jobRead.aggregatedStats.recordsCommitted)
  }

  @ParameterizedTest
  @EnumSource(value = JobConfigType::class, names = ["SYNC", "RESET_CONNECTION", "REFRESH", "CLEAR"], mode = EnumSource.Mode.EXCLUDE)
  fun `from should fail to convert a JobRead object from the config api if the config type is not supported`(jobConfigType: JobConfigType) {
    val jobRead = getJobRead(jobConfigType)
    val jobInfoRead = JobInfoRead()
    jobInfoRead.job = jobRead

    assertThrows<IllegalArgumentException> { JobResponseMapper.from(jobInfoRead) }
  }

  private fun getJobRead(jobConfigType: JobConfigType): JobRead =
    JobRead().apply {
      this.id = 1L
      this.status = JobStatus.FAILED
      this.configId = UUID.randomUUID().toString()
      this.configType = jobConfigType
      this.createdAt = 1L
      this.updatedAt = 2L
      this.aggregatedStats =
        JobAggregatedStats().apply {
          this.bytesCommitted = 12345
          this.recordsCommitted = 67890
        }
    }
}
