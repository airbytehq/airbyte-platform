package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.JobAggregatedStats
import io.airbyte.api.model.generated.JobConfigType
import io.airbyte.api.model.generated.JobInfoRead
import io.airbyte.api.model.generated.JobRead
import io.airbyte.api.model.generated.JobStatus
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class JobResponseMapperTest {
  @Test
  fun `from should convert a JobRead object from the config api to a JobResponse`() {
    // Given
    val jobRead =
      JobRead().apply {
        this.id = 1L
        this.status = JobStatus.FAILED
        this.configId = UUID.randomUUID().toString()
        this.configType = JobConfigType.SYNC
        this.createdAt = 1L
        this.updatedAt = 2L
        this.aggregatedStats =
          JobAggregatedStats().apply {
            this.bytesCommitted = 12345
            this.recordsCommitted = 67890
          }
      }
    val jobInfoRead = JobInfoRead()
    jobInfoRead.job = jobRead

    // When
    val jobResponse = JobResponseMapper.from(jobInfoRead)

    // Then
    assertEquals(jobResponse.jobId, jobRead.id)
    assertEquals(jobResponse.status.toString(), jobRead.status.toString())
    assertEquals(jobResponse.connectionId, UUID.fromString(jobRead.configId))
    assertEquals(jobResponse.jobType.toString(), jobRead.configType.toString())
    assertEquals(jobResponse.startTime, "1970-01-01T00:00:01Z")
    assertEquals(jobResponse.lastUpdatedAt, "1970-01-01T00:00:02Z")
    assertEquals(jobResponse.duration, "PT1S")
    assertEquals(jobResponse.bytesSynced, jobRead.aggregatedStats.bytesCommitted)
    assertEquals(jobResponse.rowsSynced, jobRead.aggregatedStats.recordsCommitted)
  }
}
