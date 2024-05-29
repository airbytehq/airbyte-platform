package io.airbyte.workers.internal.bookkeeping.streamstatus

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.StreamStatusesApi
import io.airbyte.api.client.model.generated.StreamStatusIncompleteRunCause
import io.airbyte.api.client.model.generated.StreamStatusJobType
import io.airbyte.api.client.model.generated.StreamStatusRead
import io.airbyte.api.client.model.generated.StreamStatusRunState
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.workers.context.ReplicationContext
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Clock
import java.util.UUID

class StreamStatusCachingApiClientTest {
  private lateinit var client: StreamStatusCachingApiClient

  private lateinit var rawClient: StreamStatusesApi
  private lateinit var rawClientWrapper: AirbyteApiClient
  private lateinit var metricClient: MetricClient
  private lateinit var clock: Clock

  @BeforeEach
  fun setup() {
    rawClient = mockk()
    rawClientWrapper = mockk { every { streamStatusesApi } returns rawClient }
    metricClient = mockk()
    clock = mockk()
    every { clock.millis() } returns Fixtures.nowMillis

    client = StreamStatusCachingApiClient(rawClientWrapper, metricClient, clock)
    client.init(Fixtures.syncCtx)
  }

  @Test
  fun createsStatusIfNotPresentInCache() {
    every { rawClient.createStreamStatus(any()) } returns Fixtures.streamStatusRead(StreamStatusRunState.RUNNING)

    client.put(Fixtures.key1, StreamStatusRunState.RUNNING)

    verify(exactly = 1) { rawClient.createStreamStatus(any()) }
  }

  @Test
  fun updatesStatusIfPresentInCache() {
    every { rawClient.createStreamStatus(any()) } returns Fixtures.streamStatusRead(StreamStatusRunState.RUNNING)
    every { rawClient.updateStreamStatus(any()) } returns Fixtures.streamStatusRead(StreamStatusRunState.RUNNING)

    client.put(Fixtures.key1, StreamStatusRunState.RUNNING)
    client.put(Fixtures.key1, StreamStatusRunState.COMPLETE)

    verify(exactly = 1) { rawClient.updateStreamStatus(any()) }
  }

  @Test
  fun ignoresDuplicates() {
    every { rawClient.createStreamStatus(any()) } returns Fixtures.streamStatusRead(StreamStatusRunState.RUNNING)

    client.put(Fixtures.key1, StreamStatusRunState.RUNNING)
    client.put(Fixtures.key1, StreamStatusRunState.RUNNING)

    verify(exactly = 0) { rawClient.updateStreamStatus(any()) }
  }

  @Test
  fun noopsAndRecordsMetricIfNotInitialized() {
    every { metricClient.count(any(), any(), *anyVararg()) } returns Unit

    val client1 = StreamStatusCachingApiClient(rawClientWrapper, metricClient, clock)
    client1.put(Fixtures.key1, StreamStatusRunState.RUNNING)

    verify(exactly = 0) { rawClient.createStreamStatus(any()) }
    verify(exactly = 1) { metricClient.count(any(), any(), *anyVararg()) }
  }

  @Test
  fun buildCreateAndUpdateReqHandleJobType() {
    val client1 = StreamStatusCachingApiClient(rawClientWrapper, metricClient, clock)
    client1.init(Fixtures.resetCtx)
    val client2 = StreamStatusCachingApiClient(rawClientWrapper, metricClient, clock)
    client2.init(Fixtures.syncCtx)

    val createResult1 = client1.buildCreateReq("namespace", "name", StreamStatusRunState.RUNNING)
    val createResult2 = client2.buildCreateReq("namespace", "name", StreamStatusRunState.RUNNING)
    val updateResult1 = client1.buildUpdateReq(UUID.randomUUID(), "namespace", "name", StreamStatusRunState.RUNNING)
    val updateResult2 = client2.buildUpdateReq(UUID.randomUUID(), "namespace", "name", StreamStatusRunState.RUNNING)
    Assertions.assertEquals(StreamStatusJobType.RESET, createResult1.jobType)
    Assertions.assertEquals(StreamStatusJobType.SYNC, createResult2.jobType)
    Assertions.assertEquals(StreamStatusJobType.RESET, updateResult1.jobType)
    Assertions.assertEquals(StreamStatusJobType.SYNC, updateResult2.jobType)
  }

  @Test
  fun buildCreateAndUpdateReqSetIncompleteRunCauseToFailed() {
    val createResult1 = client.buildCreateReq("namespace", "name", StreamStatusRunState.INCOMPLETE)
    val createResult2 = client.buildCreateReq("namespace", "name", StreamStatusRunState.RUNNING)
    val updateResult1 = client.buildUpdateReq(UUID.randomUUID(), "namespace", "name", StreamStatusRunState.INCOMPLETE)
    val updateResult2 = client.buildUpdateReq(UUID.randomUUID(), "namespace", "name", StreamStatusRunState.RUNNING)

    Assertions.assertEquals(StreamStatusIncompleteRunCause.FAILED, createResult1.incompleteRunCause)
    Assertions.assertNull(createResult2.incompleteRunCause)
    Assertions.assertEquals(StreamStatusIncompleteRunCause.FAILED, updateResult1.incompleteRunCause)
    Assertions.assertNull(updateResult2.incompleteRunCause)
  }

  object Fixtures {
    val key1 = StreamStatusKey(streamName = "test-stream-1", streamNamespace = null)
    val nowMillis = System.currentTimeMillis()

    fun streamStatusRead(runState: StreamStatusRunState): StreamStatusRead =
      StreamStatusRead(
        attemptNumber = 1,
        connectionId = UUID.randomUUID(),
        id = UUID.randomUUID(),
        jobId = 1L,
        jobType = StreamStatusJobType.SYNC,
        runState = runState,
        streamName = "name",
        streamNamespace = "namespace",
        transitionedAt = System.currentTimeMillis(),
        workspaceId = UUID.randomUUID(),
        incompleteRunCause = null,
        metadata = null,
      )

    val syncCtx = ctx(false)
    val resetCtx = ctx(true)

    private fun ctx(isReset: Boolean) =
      ReplicationContext(
        isReset,
        UUID.randomUUID(),
        UUID.randomUUID(),
        UUID.randomUUID(),
        12L,
        34,
        UUID.randomUUID(),
        "source-image",
        "dest-image",
        UUID.randomUUID(),
        UUID.randomUUID(),
      )
  }
}
