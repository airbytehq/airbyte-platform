/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.bookkeeping.streamstatus

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.StreamStatusesApi
import io.airbyte.api.client.model.generated.StreamStatusIncompleteRunCause
import io.airbyte.api.client.model.generated.StreamStatusJobType
import io.airbyte.api.client.model.generated.StreamStatusRateLimitedMetadata
import io.airbyte.api.client.model.generated.StreamStatusRead
import io.airbyte.api.client.model.generated.StreamStatusRunState
import io.airbyte.container.orchestrator.worker.context.ReplicationContext
import io.airbyte.metrics.MetricClient
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

    client = StreamStatusCachingApiClient(rawClientWrapper, clock)
  }

  @Test
  fun createsStatusIfNotPresentInCache() {
    every { rawClient.createStreamStatus(any()) } returns Fixtures.streamStatusRead(StreamStatusRunState.RUNNING)

    client.put(Fixtures.cache(), Fixtures.key1, StreamStatusRunState.RUNNING, null, Fixtures.syncCtx)

    verify(exactly = 1) { rawClient.createStreamStatus(any()) }
  }

  @Test
  fun updatesStatusIfPresentInCache() {
    every { rawClient.createStreamStatus(any()) } returns Fixtures.streamStatusRead(StreamStatusRunState.RUNNING)
    every { rawClient.updateStreamStatus(any()) } returns Fixtures.streamStatusRead(StreamStatusRunState.RUNNING)

    val cache = Fixtures.cache()

    client.put(cache, Fixtures.key1, StreamStatusRunState.RUNNING, null, Fixtures.syncCtx)
    client.put(cache, Fixtures.key1, StreamStatusRunState.COMPLETE, null, Fixtures.syncCtx)

    verify(exactly = 1) { rawClient.updateStreamStatus(any()) }
  }

  @Test
  fun ignoresDuplicates() {
    every { rawClient.createStreamStatus(any()) } returns Fixtures.streamStatusRead(StreamStatusRunState.RUNNING)

    client.put(Fixtures.cache(), Fixtures.key1, StreamStatusRunState.RUNNING, null, Fixtures.syncCtx)
    client.put(Fixtures.cache(), Fixtures.key1, StreamStatusRunState.RUNNING, null, Fixtures.syncCtx)

    verify(exactly = 0) { rawClient.updateStreamStatus(any()) }
  }

  @Test
  fun buildCreateAndUpdateReqHandleJobType() {
    val client1 = StreamStatusCachingApiClient(rawClientWrapper, clock)

    val createResult1 = client1.buildCreateReq("namespace", "name", Fixtures.resetCtx, StreamStatusRunState.RUNNING)
    val createResult2 = client1.buildCreateReq("namespace", "name", Fixtures.syncCtx, StreamStatusRunState.RUNNING)
    val updateResult1 = client1.buildUpdateReq(UUID.randomUUID(), "namespace", "name", Fixtures.resetCtx, StreamStatusRunState.RUNNING)
    val updateResult2 = client1.buildUpdateReq(UUID.randomUUID(), "namespace", "name", Fixtures.syncCtx, StreamStatusRunState.RUNNING)
    Assertions.assertEquals(StreamStatusJobType.RESET, createResult1.jobType)
    Assertions.assertEquals(StreamStatusJobType.SYNC, createResult2.jobType)
    Assertions.assertEquals(StreamStatusJobType.RESET, updateResult1.jobType)
    Assertions.assertEquals(StreamStatusJobType.SYNC, updateResult2.jobType)
  }

  @Test
  fun buildCreateAndUpdateReqSetIncompleteRunCauseToFailed() {
    val createResult1 = client.buildCreateReq("namespace", "name", Fixtures.syncCtx, StreamStatusRunState.INCOMPLETE)
    val createResult2 = client.buildCreateReq("namespace", "name", Fixtures.syncCtx, StreamStatusRunState.RUNNING)
    val updateResult1 = client.buildUpdateReq(UUID.randomUUID(), "namespace", "name", Fixtures.syncCtx, StreamStatusRunState.INCOMPLETE)
    val updateResult2 = client.buildUpdateReq(UUID.randomUUID(), "namespace", "name", Fixtures.syncCtx, StreamStatusRunState.RUNNING)

    Assertions.assertEquals(StreamStatusIncompleteRunCause.FAILED, createResult1.incompleteRunCause)
    Assertions.assertNull(createResult2.incompleteRunCause)
    Assertions.assertEquals(StreamStatusIncompleteRunCause.FAILED, updateResult1.incompleteRunCause)
    Assertions.assertNull(updateResult2.incompleteRunCause)
  }

  @Test
  fun buildCreateAndUpdateReqHandleMetadata() {
    val metadata1 = StreamStatusRateLimitedMetadata(quotaReset = 123L)
    val metadata2 = StreamStatusRateLimitedMetadata(quotaReset = 456L)

    val createResult1 = client.buildCreateReq("namespace", "name", Fixtures.syncCtx, StreamStatusRunState.INCOMPLETE, metadata1)
    val updateResult1 = client.buildUpdateReq(UUID.randomUUID(), "namespace", "name", Fixtures.syncCtx, StreamStatusRunState.RUNNING, metadata2)

    Assertions.assertEquals(metadata1, createResult1.metadata)
    Assertions.assertEquals(metadata2, updateResult1.metadata)
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

    fun cache() = mutableMapOf<StreamStatusKey, StreamStatusRead>()

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
