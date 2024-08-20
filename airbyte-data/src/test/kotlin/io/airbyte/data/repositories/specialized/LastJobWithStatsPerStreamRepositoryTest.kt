package io.airbyte.data.repositories.specialized

import io.airbyte.data.repositories.AbstractConfigRepositoryTest
import io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS
import io.airbyte.db.instance.jobs.jooq.generated.Tables.STREAM_STATUSES
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobConfigType
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusJobType
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusRunState
import io.airbyte.db.instance.jobs.jooq.generated.tables.records.JobsRecord
import io.airbyte.db.instance.jobs.jooq.generated.tables.records.StreamStatusesRecord
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.util.UUID

internal class LastJobWithStatsPerStreamRepositoryTest : AbstractConfigRepositoryTest() {
  companion object {
    private val job1Id = 1L
    private val job2Id = 2L
    private val job3Id = 3L

    private val connectionId = UUID.randomUUID()
    private val streamNameFoo = "foo"
    private val streamNameBar = "bar"
    private val streamNamespace1 = "ns1"
    private val streamNamespace2 = "ns2"
  }

  @Test
  fun `test getLastJobPerStream`() {
    setupFixtures()

    val result =
      lastJobPerStreamRepository.findLastJobIdWithStatsPerStream(
        connectionId,
      ).associate { listOf(it.streamName, it.streamNamespace) to it.jobId }

    val actualFooNs1JobId = result[listOf(streamNameFoo, streamNamespace1)]
    val actualBarNs1JobId = result[listOf(streamNameBar, streamNamespace1)]
    val actualFooNs2JobId = result[listOf(streamNameFoo, streamNamespace2)]
    val actualFooNullJobId = result[listOf(streamNameFoo, null)]

    // make sure we only get a result for the streams we asked for
    assertEquals(4, result.keys.size)

    // make sure we get the correct job id for each stream based on the fixture data
    assertEquals(job1Id, actualFooNs1JobId)
    assertEquals(job3Id, actualBarNs1JobId)
    assertEquals(job2Id, actualFooNs2JobId)
    assertEquals(job1Id, actualFooNullJobId)
  }

  private fun setupFixtures() {
    val job1 = jobRecord(job1Id, connectionId)
    val job2 = jobRecord(job2Id, connectionId)
    val job3 = jobRecord(job3Id, connectionId)
    jooqDslContext.batchInsert(job1, job2, job3).execute()

    val job1FooNs1 = streamStatusesRecord(job1Id, streamNameFoo, streamNamespace1, connectionId)
    val job1FooNsNull = streamStatusesRecord(job1Id, streamNameFoo, null, connectionId)
    val job1FooNs2 = streamStatusesRecord(job1Id, streamNameFoo, streamNamespace2, connectionId)
    val job1BarNs1 = streamStatusesRecord(job1Id, streamNameBar, streamNamespace1, connectionId)
    val job2FooNs2 = streamStatusesRecord(job2Id, streamNameFoo, streamNamespace2, connectionId)
    val job3BarNs1 = streamStatusesRecord(job3Id, streamNameBar, streamNamespace1, connectionId)
    val job3BarNs1RandomConn = streamStatusesRecord(job3Id, streamNameBar, streamNamespace1, UUID.randomUUID()) // make sure this doesn't show up
    jooqDslContext.batchInsert(job1FooNs1, job1FooNsNull, job1FooNs2, job1BarNs1, job2FooNs2, job3BarNs1, job3BarNs1RandomConn).execute()
  }

  private fun streamStatusesRecord(
    jobId: Long,
    streamName: String,
    streamNamespace: String?,
    connectionId: UUID,
  ): StreamStatusesRecord {
    return jooqDslContext.newRecord(STREAM_STATUSES).apply {
      this.id = UUID.randomUUID()
      this.jobId = jobId
      this.jobType = JobStreamStatusJobType.sync
      this.connectionId = connectionId
      this.streamName = streamName
      this.streamNamespace = streamNamespace
      this.workspaceId = UUID.randomUUID()
      this.attemptNumber = 0
      this.runState = JobStreamStatusRunState.complete
      this.transitionedAt = OffsetDateTime.now()
    }
  }

  private fun jobRecord(
    jobId: Long,
    connectionId: UUID,
  ): JobsRecord {
    return AbstractConfigRepositoryTest.jooqDslContext.newRecord(JOBS).apply {
      this.id = jobId
      this.scope = connectionId.toString()
      this.configType = JobConfigType.sync
      this.status = JobStatus.succeeded
    }
  }
}
