package io.airbyte.data.services.impls.data

import io.airbyte.config.StreamDescriptor
import io.airbyte.data.repositories.specialized.LastJobWithStatsPerStreamRepository
import io.airbyte.data.repositories.specialized.StreamWithLastJobId
import io.airbyte.data.services.StreamStatsService
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.UUID

class StreamStatsServiceDataImplTest {
  private lateinit var lastJobWithStatsPerStreamRepository: LastJobWithStatsPerStreamRepository
  private lateinit var streamStatsService: StreamStatsService

  @BeforeEach
  fun setup() {
    lastJobWithStatsPerStreamRepository = mockk()
    streamStatsService = StreamStatsServiceDataImpl(lastJobWithStatsPerStreamRepository)
  }

  @Nested
  inner class GetLastJobIdWithStatsByStream {
    private val connectionId = UUID.randomUUID()
    private val job1Id = 1L
    private val job2Id = 2L
    private val stream1 = StreamDescriptor().withName("stream_1").withNamespace("namespace_1")
    private val stream2 = StreamDescriptor().withName("stream_2").withNamespace("namespace_1")
    private val stream1NoNamespace = StreamDescriptor().withName("stream_1")

    @Test
    fun `should return last job id with stats by stream descriptor`() {
      every {
        lastJobWithStatsPerStreamRepository.findLastJobIdWithStatsPerStream(
          connectionId,
          arrayOf(stream1.name, stream2.name, stream1NoNamespace.name),
          arrayOf(stream1.namespace, stream2.namespace, null),
        )
      } returns
        listOf(
          StreamWithLastJobId(jobId = job1Id, streamName = stream1.name, streamNamespace = stream1.namespace),
          StreamWithLastJobId(jobId = job1Id, streamName = stream2.name, streamNamespace = stream2.namespace),
          StreamWithLastJobId(jobId = job2Id, streamName = stream1NoNamespace.name, streamNamespace = null),
        )

      val result =
        streamStatsService.getLastJobIdWithStatsByStream(
          connectionId = connectionId,
          streams = listOf(stream1, stream2, stream1NoNamespace),
        )

      assertEquals(3, result.size)
      assertEquals(job1Id, result[stream1])
      assertEquals(job1Id, result[stream2])
      assertEquals(job2Id, result[stream1NoNamespace])
    }
  }
}
