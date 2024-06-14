package io.airbyte.data.repositories.specialized

import io.airbyte.data.repositories.AbstractConfigRepositoryTest
import io.airbyte.data.repositories.entities.Attempt
import io.airbyte.data.repositories.entities.Job
import io.airbyte.data.repositories.entities.StreamStats
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

internal class LastJobWithStatsPerStreamRepositoryTest : AbstractConfigRepositoryTest() {
  companion object {
    private val job1Id = 1L
    private val job2Id = 2L
    private val job3Id = 3L

    private val job1Attempt1Id = 1L
    private val job1Attempt2Id = 2L
    private val job2Attempt1Id = 3L
    private val job3Attempt1Id = 4L

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
        arrayOf(streamNameFoo, streamNameBar, streamNameFoo, streamNameFoo),
        arrayOf(streamNamespace1, streamNamespace1, streamNamespace2, null),
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
    val job1 = Job(id = job1Id, scope = connectionId.toString())
    val job2 = Job(id = job2Id, scope = connectionId.toString())
    val job3 = Job(id = job3Id, scope = connectionId.toString())

    val job1Attempt1 =
      Attempt(
        id = job1Attempt1Id,
        jobId = job1Id,
        attemptNumber = 1,
      )
    val job1Attempt2 =
      Attempt(
        id = job1Attempt2Id,
        jobId = job1Id,
        attemptNumber = 2,
      )

    val job2Attempt1 =
      Attempt(
        id = job2Attempt1Id,
        jobId = job2Id,
        attemptNumber = 1,
      )

    val job3Attempt1 =
      Attempt(
        id = job3Attempt1Id,
        jobId = job3Id,
        attemptNumber = 1,
      )

    // stream foo_namespace1 only exists in job 1, it has stats in both attempts
    val fooNs1Job1Attempt1Stats =
      StreamStats(
        attemptId = job1Attempt1.id!!,
        streamName = streamNameFoo,
        streamNamespace = streamNamespace1,
        recordsEmitted = 10,
        bytesEmitted = 100,
        connectionId = connectionId,
      )
    val fooNs1Job1Attempt2Stats =
      StreamStats(
        attemptId = job1Attempt2.id!!,
        streamName = streamNameFoo,
        streamNamespace = streamNamespace1,
        recordsEmitted = 20,
        bytesEmitted = 200,
        connectionId = connectionId,
      )

    // stream bar_namespace1 exists in all three jobs
    val barNs1Job1Attempt1Stats =
      StreamStats(
        attemptId = job1Attempt1.id!!,
        streamName = streamNameBar,
        streamNamespace = streamNamespace1,
        recordsEmitted = 30,
        bytesEmitted = 300,
        connectionId = connectionId,
      )

    val barNs1Job1Attempt2Stats =
      StreamStats(
        attemptId = job1Attempt2.id!!,
        streamName = streamNameBar,
        streamNamespace = streamNamespace1,
        recordsEmitted = 40,
        bytesEmitted = 400,
        connectionId = connectionId,
      )

    val barNs1Job2Attempt1Stats =
      StreamStats(
        attemptId = job2Attempt1.id!!,
        streamName = streamNameBar,
        streamNamespace = streamNamespace1,
        recordsEmitted = 50,
        bytesEmitted = 500,
        connectionId = connectionId,
      )

    val barNs1Job3Attempt1Stats =
      StreamStats(
        attemptId = job3Attempt1.id!!,
        streamName = streamNameBar,
        streamNamespace = streamNamespace1,
        recordsEmitted = 60,
        bytesEmitted = 600,
        connectionId = connectionId,
      )

    // stream foo_namespace2 exists in job1 and job 2. Job1 has two attempts, job2 has one attempt.
    // Only the 2nd attempt of job1 has stats for foo_namespace2. The only attempt of job2 has stats for foo_namespace2.
    val fooNs2Job1Attempt1Stats =
      StreamStats(
        attemptId = job1Attempt2.id!!,
        streamName = streamNameFoo,
        streamNamespace = streamNamespace2,
        recordsEmitted = 70,
        bytesEmitted = 700,
        connectionId = connectionId,
      )

    val fooNs2Job2Attempt1Stats =
      StreamStats(
        attemptId = job2Attempt1.id!!,
        streamName = streamNameFoo,
        streamNamespace = streamNamespace2,
        recordsEmitted = 80,
        bytesEmitted = 800,
        connectionId = connectionId,
      )

    // stream foo_<null> only exists in job 1, and only has stats in the second attempt of job 1
    val fooNullJob1Attempt1Stats =
      StreamStats(
        attemptId = job1Attempt2.id!!,
        streamName = streamNameFoo,
        streamNamespace = null,
        recordsEmitted = 90,
        bytesEmitted = 900,
        connectionId = connectionId,
      )

    // stream bar_namespace2 only exists in job 1. We save this record to the database, but
    // it should not be returned in the query because it is not in the list of streams we are querying for.
    val barNs2Job1Attempt1Stats =
      StreamStats(
        attemptId = job1Attempt1.id!!,
        streamName = streamNameBar,
        streamNamespace = streamNamespace2,
        recordsEmitted = 100,
        bytesEmitted = 1000,
        connectionId = connectionId,
      )

    jobsRepository.saveAll(listOf(job1, job2, job3))
    attemptsRepository.saveAll(listOf(job1Attempt1, job1Attempt2, job2Attempt1, job3Attempt1))
    streamStatsRepository.saveAll(
      listOf(
        fooNs1Job1Attempt1Stats,
        fooNs1Job1Attempt2Stats,
        barNs1Job1Attempt1Stats,
        barNs1Job1Attempt2Stats,
        barNs1Job2Attempt1Stats,
        barNs1Job3Attempt1Stats,
        fooNs2Job1Attempt1Stats,
        fooNs2Job2Attempt1Stats,
        fooNullJob1Attempt1Stats,
        barNs2Job1Attempt1Stats,
      ),
    )
  }
}
