/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

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
    private const val JOB_ID_1 = 1L
    private const val JOB_ID_2 = 2L
    private const val JOB_ID_3 = 3L

    private val connectionId: UUID = UUID.randomUUID()
    private const val STREAM_NAME_FOO = "foo"
    private const val STREAM_NAME_BAR = "bar"

    private const val STREAM_NAMESPACE_1 = "ns1"
    private const val STREAM_NAMESPACE_2 = "ns2"
  }

  @Test
  fun `test getLastJobPerStream`() {
    setupFixtures()

    val result =
      lastJobPerStreamRepository
        .findLastJobIdWithStatsPerStream(
          connectionId,
        ).associate { listOf(it.streamName, it.streamNamespace) to it.jobId }

    val actualFooNs1JobId = result[listOf(STREAM_NAME_FOO, STREAM_NAMESPACE_1)]
    val actualBarNs1JobId = result[listOf(STREAM_NAME_BAR, STREAM_NAMESPACE_1)]
    val actualFooNs2JobId = result[listOf(STREAM_NAME_FOO, STREAM_NAMESPACE_2)]
    val actualFooNullJobId = result[listOf(STREAM_NAME_FOO, null)]

    // make sure we only get a result for the streams we asked for
    assertEquals(4, result.keys.size)

    // make sure we get the correct job id for each stream based on the fixture data
    assertEquals(JOB_ID_1, actualFooNs1JobId)
    assertEquals(JOB_ID_3, actualBarNs1JobId)
    assertEquals(JOB_ID_2, actualFooNs2JobId)
    assertEquals(JOB_ID_1, actualFooNullJobId)
  }

  private fun setupFixtures() {
    val job1 = jobRecord(JOB_ID_1, connectionId)
    val job2 = jobRecord(JOB_ID_2, connectionId)
    val job3 = jobRecord(JOB_ID_3, connectionId)
    jooqDslContext.batchInsert(job1, job2, job3).execute()

    val job1FooNs1 = streamStatusesRecord(JOB_ID_1, STREAM_NAME_FOO, STREAM_NAMESPACE_1, connectionId)
    val job1FooNsNull = streamStatusesRecord(JOB_ID_1, STREAM_NAME_FOO, null, connectionId)
    val job1FooNs2 = streamStatusesRecord(JOB_ID_1, STREAM_NAME_FOO, STREAM_NAMESPACE_2, connectionId)
    val job1BarNs1 = streamStatusesRecord(JOB_ID_1, STREAM_NAME_BAR, STREAM_NAMESPACE_1, connectionId)
    val job2FooNs2 = streamStatusesRecord(JOB_ID_2, STREAM_NAME_FOO, STREAM_NAMESPACE_2, connectionId)
    val job3BarNs1 = streamStatusesRecord(JOB_ID_3, STREAM_NAME_BAR, STREAM_NAMESPACE_1, connectionId)
    val job3BarNs1RandomConn =
      streamStatusesRecord(
        JOB_ID_3,
        STREAM_NAME_BAR,
        STREAM_NAMESPACE_1,
        UUID.randomUUID(),
      ) // make sure this doesn't show up
    jooqDslContext.batchInsert(job1FooNs1, job1FooNsNull, job1FooNs2, job1BarNs1, job2FooNs2, job3BarNs1, job3BarNs1RandomConn).execute()
  }

  private fun streamStatusesRecord(
    jobId: Long,
    streamName: String,
    streamNamespace: String?,
    connectionId: UUID,
  ): StreamStatusesRecord =
    jooqDslContext.newRecord(STREAM_STATUSES).apply {
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

  private fun jobRecord(
    jobId: Long,
    connectionId: UUID,
  ): JobsRecord =
    AbstractConfigRepositoryTest.jooqDslContext.newRecord(JOBS).apply {
      this.id = jobId
      this.scope = connectionId.toString()
      this.configType = JobConfigType.sync
      this.status = JobStatus.succeeded
    }
}
