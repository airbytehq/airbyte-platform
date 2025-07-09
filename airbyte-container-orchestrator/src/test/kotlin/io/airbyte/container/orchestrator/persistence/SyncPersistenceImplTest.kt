/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.persistence

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.AttemptApi
import io.airbyte.api.client.generated.StateApi
import io.airbyte.api.client.model.generated.AttemptStats
import io.airbyte.api.client.model.generated.AttemptStreamStats
import io.airbyte.api.client.model.generated.ConnectionState
import io.airbyte.api.client.model.generated.ConnectionStateCreateOrUpdate
import io.airbyte.api.client.model.generated.ConnectionStateType
import io.airbyte.api.client.model.generated.SaveStatsRequestBody
import io.airbyte.api.client.model.generated.StreamDescriptor
import io.airbyte.api.client.model.generated.StreamState
import io.airbyte.commons.json.Jsons
import io.airbyte.container.orchestrator.bookkeeping.ParallelStreamStatsTracker
import io.airbyte.container.orchestrator.bookkeeping.StateCheckSumCountEventHandler
import io.airbyte.container.orchestrator.bookkeeping.SyncStatsTracker
import io.airbyte.container.orchestrator.bookkeeping.state.DefaultStateAggregator
import io.airbyte.container.orchestrator.bookkeeping.state.SingleStateAggregator
import io.airbyte.container.orchestrator.bookkeeping.state.StateAggregator
import io.airbyte.container.orchestrator.bookkeeping.state.StreamStateAggregator
import io.airbyte.metrics.MetricClient
import io.airbyte.protocol.models.v0.AirbyteEstimateTraceMessage
import io.airbyte.protocol.models.v0.AirbyteRecordMessage
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStreamNameNamespacePair
import io.airbyte.protocol.models.v0.AirbyteStreamState
import io.airbyte.workers.models.ArchitectureConstants
import io.mockk.CapturingSlot
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.UUID
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

private const val FLUSH_PERIOD = 10L

internal class SyncPersistenceImplTest {
  private lateinit var syncPersistence: SyncPersistenceImpl
  private lateinit var syncStatsTracker: SyncStatsTracker
  private lateinit var stateApiClient: StateApi
  private lateinit var attemptApiClient: AttemptApi
  private lateinit var executorService: ScheduledExecutorService
  private lateinit var actualFlushMethod: CapturingSlot<Runnable>
  private lateinit var stateAggregator: StateAggregator
  private lateinit var connectionId: UUID
  private var jobId: Long = 0L
  private var attemptNumber: Int = 0
  private lateinit var airbyteApiClient: AirbyteApiClient
  private lateinit var metricClient: MetricClient

  @BeforeEach
  fun beforeEach() {
    connectionId = UUID.randomUUID()
    jobId = (Math.random() * Long.Companion.MAX_VALUE).toLong()
    attemptNumber = (Math.random() * Int.Companion.MAX_VALUE).toInt()
    metricClient = mockk<MetricClient>(relaxed = true)

    stateAggregator = DefaultStateAggregator(StreamStateAggregator(), SingleStateAggregator())

    // Setting up a slot to be able to manually trigger the actual flush method rather than
    // relying on the ScheduledExecutorService and having to deal with Thread.sleep in the tests.
    actualFlushMethod = slot<Runnable>()

    // Wire the executor service with arg captures
    executorService =
      mockk<ScheduledExecutorService> {
        every { awaitTermination(any(), any()) } returns true
        every {
          scheduleAtFixedRate(capture(actualFlushMethod), 0L, FLUSH_PERIOD, TimeUnit.SECONDS)
        } returns mockk<ScheduledFuture<*>>(relaxed = true)
        every { shutdown() } returns Unit
      }

    syncStatsTracker =
      spyk(
        ParallelStreamStatsTracker(
          metricClient = metricClient,
          stateCheckSumEventHandler = mockk<StateCheckSumCountEventHandler>(relaxed = true),
          ArchitectureConstants.ORCHESTRATOR,
        ),
      )

    // Setting syncPersistence
    stateApiClient =
      mockk<StateApi> {
        every { createOrUpdateState(any()) } returns mockk(relaxed = true)
      }
    attemptApiClient =
      mockk<AttemptApi> {
        every { saveStats(any()) } returns mockk(relaxed = true)
      }
    airbyteApiClient =
      mockk<AirbyteApiClient> {
        every { attemptApi } returns attemptApiClient
        every { stateApi } returns stateApiClient
      }

    syncPersistence =
      SyncPersistenceImpl(
        airbyteApiClient = airbyteApiClient,
        stateBuffer = stateAggregator,
        stateFlushExecutorService = executorService,
        stateFlushPeriodInSeconds = FLUSH_PERIOD,
        metricClient = metricClient,
        syncStatsTracker = syncStatsTracker,
        connectionId = connectionId,
        jobId = jobId,
        attemptNumber = attemptNumber,
      )
  }

  @AfterEach
  fun afterEach() {
    clearInvocations(listOf(attemptApiClient, stateApiClient))
    syncPersistence.close()
  }

  @Test
  @Throws(IOException::class)
  fun testHappyPath() {
    val stateA1 = getStreamState("A", 1)
    syncPersistence.accept(connectionId, stateA1)
    clearInvocations(listOf(executorService, stateApiClient))

    // Simulating the expected flush execution
    actualFlushMethod.captured.run()
    verifyStateUpdateApiCall(listOf(stateA1))
    clearInvocations(listOf(stateApiClient))

    val stateB1 = getStreamState("B", 1)
    val stateC2 = getStreamState("C", 2)
    syncPersistence.accept(connectionId, stateB1)
    syncPersistence.accept(connectionId, stateC2)

    // This should only happen the first time before we schedule the task
    verify(exactly = 0) { stateApiClient.getState(any()) }

    // Forcing a second flush
    actualFlushMethod.captured.run()
    verifyStateUpdateApiCall(listOf(stateB1, stateC2))
    clearInvocations(mocks = listOf(stateApiClient))

    // Forcing another flush without data to flush
    actualFlushMethod.captured.run()
    verify(exactly = 0) { stateApiClient.createOrUpdateState(any()) }
    clearInvocations(mocks = listOf(stateApiClient))

    // scheduleAtFixedRate should not have received any other calls
    verify(exactly = 0) { executorService.scheduleAtFixedRate(any(), any(), any(), any()) }
  }

  @Test
  @Throws(IOException::class)
  fun testFlushWithApiFailures() {
    val stateF1 = getStreamState("F", 1)
    syncPersistence.accept(connectionId, stateF1)

    // Set API call to fail
    every { stateApiClient.createOrUpdateState(any()) } throws IOException()

    // Flushing
    actualFlushMethod.captured.run()
    verifyStateUpdateApiCall(listOf(stateF1))
    clearInvocations(listOf(stateApiClient))

    // Adding more states
    val stateG1 = getStreamState("G", 1)
    syncPersistence.accept(connectionId, stateG1)

    // Flushing again
    actualFlushMethod.captured.run()
    verifyStateUpdateApiCall(listOf(stateF1, stateG1))
    clearInvocations(listOf(stateApiClient))

    // Adding more states
    val stateF2 = getStreamState("F", 2)
    syncPersistence.accept(connectionId, stateF2)

    // Flushing again
    actualFlushMethod.captured.run()
    verifyStateUpdateApiCall(listOf(stateF2, stateG1))
    clearInvocations(listOf(stateApiClient))

    // Clear the error state from the API
    every { stateApiClient.createOrUpdateState(any()) } returns mockk(relaxed = true)

    // Flushing again
    actualFlushMethod.captured.run()
    verifyStateUpdateApiCall(listOf(stateF2, stateG1))
    clearInvocations(listOf(stateApiClient))

    // Sanity check Flushing again should not trigger an API call since all the data has been
    // successfully flushed
    actualFlushMethod.captured.run()
    verify(exactly = 0) { stateApiClient.createOrUpdateState(any()) }
  }

  @Test
  @Throws(IOException::class)
  fun testStatsFlushBasicEmissions() {
    syncPersistence.updateStats(AirbyteRecordMessage())
    syncPersistence.accept(connectionId, getStreamState("a", 1))

    actualFlushMethod.captured.run()
    verify(exactly = 1) { stateApiClient.createOrUpdateState(any()) }
    verify(exactly = 1) { attemptApiClient.saveStats(any()) }
    clearInvocations(listOf(stateApiClient, attemptApiClient))

    // We emit stats even if there is no state to persist
    syncPersistence.updateStats(AirbyteRecordMessage().withStream("stream1"))
    actualFlushMethod.captured.run()
    verify(exactly = 0) { stateApiClient.createOrUpdateState(any()) }
    verify(exactly = 1) { attemptApiClient.saveStats(any()) }
  }

  @Test
  fun testStatsFlush() {
    // Setup so that the flush does something
    syncPersistence.updateStats(AirbyteRecordMessage())
    syncPersistence.accept(connectionId, getStreamState("s1", 1))

    val sd1 = AirbyteStreamNameNamespacePair("s1", "ns1")
    val sd2 = AirbyteStreamNameNamespacePair("s2", "ns1")

    every { syncStatsTracker.getStats() } returns
      mapOf(
        sd1 to
          mockk(relaxed = true) {
            every { bytesCommitted } returns 10
            every { bytesEmitted } returns 30
            every { recordsCommitted } returns 1
            every { recordsEmitted } returns 3
            every { recordsRejected } returns 2
          },
        sd2 to
          mockk(relaxed = true) {
            every { bytesCommitted } returns 1000
            every { bytesEmitted } returns 3000
            every { recordsCommitted } returns 100
            every { recordsEmitted } returns 300
            every { recordsRejected } returns 200
          },
      )

    actualFlushMethod.captured.run()
    val expectedSaveStats =
      SaveStatsRequestBody(
        jobId = jobId,
        attemptNumber = attemptNumber,
        stats =
          AttemptStats(
            bytesCommitted = 1010,
            bytesEmitted = 3030,
            recordsCommitted = 101,
            recordsEmitted = 303,
            recordsRejected = 202,
          ),
        streamStats =
          listOf(
            AttemptStreamStats(
              streamName = sd1.name,
              streamNamespace = sd1.namespace,
              stats =
                AttemptStats(
                  bytesCommitted = 10,
                  bytesEmitted = 30,
                  estimatedBytes = 0,
                  estimatedRecords = 0,
                  recordsCommitted = 1,
                  recordsEmitted = 3,
                  recordsRejected = 2,
                ),
            ),
            AttemptStreamStats(
              streamName = sd2.name,
              streamNamespace = sd2.namespace,
              stats =
                AttemptStats(
                  bytesCommitted = 1000,
                  bytesEmitted = 3000,
                  estimatedBytes = 0,
                  estimatedRecords = 0,
                  recordsCommitted = 100,
                  recordsEmitted = 300,
                  recordsRejected = 200,
                ),
            ),
          ),
        connectionId = connectionId,
      )
    verify(exactly = 1) { attemptApiClient.saveStats(expectedSaveStats) }
  }

  @Test
  @Throws(IOException::class)
  fun testStatsAreNotPersistedWhenStateFails() {
    // We should not save stats if persist state failed
    syncPersistence.updateStats(AirbyteRecordMessage())
    syncPersistence.accept(connectionId, getStreamState("b", 2))
    every { stateApiClient.createOrUpdateState(any()) } throws IOException()
    actualFlushMethod.captured.run()
    verify(exactly = 1) { stateApiClient.createOrUpdateState(any()) }
    verify(exactly = 0) { attemptApiClient.saveStats(any()) }
    clearInvocations(listOf(stateApiClient, attemptApiClient))
    every { stateApiClient.createOrUpdateState(any()) } returns mockk(relaxed = true)

    // Next sync should attempt to flush everything
    actualFlushMethod.captured.run()
    verify(exactly = 1) { stateApiClient.createOrUpdateState(any()) }
    verify(exactly = 1) { attemptApiClient.saveStats(any()) }
  }

  @Test
  @Throws(IOException::class)
  fun testStatsFailuresAreRetriedOnFollowingRunsEvenWithoutNewStates() {
    // If we failed to save stats, we should retry on the next schedule even if there were no new states
    syncPersistence.updateStats(AirbyteRecordMessage())
    syncPersistence.accept(connectionId, getStreamState("a", 3))
    every { attemptApiClient.saveStats(any()) } throws IOException()
    actualFlushMethod.captured.run()
    verify(exactly = 1) { stateApiClient.createOrUpdateState(any()) }
    verify(exactly = 1) { attemptApiClient.saveStats(any()) }

    clearInvocations(listOf(stateApiClient, attemptApiClient))
    every { attemptApiClient.saveStats(any()) } returns mockk(relaxed = true)

    actualFlushMethod.captured.run()
    verify(exactly = 0) { stateApiClient.createOrUpdateState(any()) }
    verify(exactly = 1) { attemptApiClient.saveStats(any()) }
  }

  @Test
  fun startsFlushThreadOnInit() {
    // syncPersistence is created and init in the @BeforeEach block
    verify(exactly = 1) { executorService.scheduleAtFixedRate(any(), 0L, FLUSH_PERIOD, TimeUnit.SECONDS) }
  }

  @Test
  @Throws(IOException::class)
  fun statsDontPersistIfTheresBeenNoChanges() {
    // update stats
    syncPersistence.updateStats(AirbyteRecordMessage().withStream("stream1"))

    // stats have updated so we should save
    actualFlushMethod.captured.run()
    verify(exactly = 1) { attemptApiClient.saveStats(any()) }
    clearInvocations(listOf(stateApiClient, attemptApiClient))

    // stats have NOT updated so we should not save
    actualFlushMethod.captured.run()
    verify(exactly = 0) { attemptApiClient.saveStats(any()) }
    clearInvocations(listOf(stateApiClient, attemptApiClient))

    // update stats for different stream
    syncPersistence.updateStats(AirbyteRecordMessage().withStream("stream2").withNamespace("other"))

    // stats have updated so we should save
    actualFlushMethod.captured.run()
    verify(exactly = 1) { attemptApiClient.saveStats(any()) }
    clearInvocations(listOf(stateApiClient, attemptApiClient))

    // stats have NOT updated so we should not save
    actualFlushMethod.captured.run()
    verify(exactly = 0) { attemptApiClient.saveStats(any()) }
    clearInvocations(listOf(stateApiClient, attemptApiClient))

    // update stats for the original stream
    syncPersistence.updateStats(AirbyteRecordMessage().withStream("stream1"))

    // stats have updated so we should save
    actualFlushMethod.captured.run()
    verify(exactly = 1) { attemptApiClient.saveStats(any()) }
    clearInvocations(listOf(stateApiClient, attemptApiClient))
  }

  @Test
  @Throws(Exception::class)
  fun testClose() {
    // Adding a state to flush, this state should get flushed when we close syncPersistence
    val stateA2 = getStreamState("A", 2)
    syncPersistence.updateStats(AirbyteRecordMessage())
    syncPersistence.accept(connectionId, stateA2)

    // Shutdown, we expect the executor service to be stopped and an stateApiClient to be called
    every { executorService.awaitTermination(any(), any()) } returns true

    syncPersistence.close()

    verify(exactly = 1) { executorService.shutdown() }
    verifyStateUpdateApiCall(listOf(stateA2))
    verify(exactly = 1) { attemptApiClient.saveStats(any()) }
  }

  @Test
  @Throws(Exception::class)
  fun testCloseMergeStatesFromPreviousFailure() {
    // Adding a state to flush, this state should get flushed when we close syncPersistence
    val stateA2 = getStreamState("closeA", 2)
    syncPersistence.accept(connectionId, stateA2)

    // Trigger a failure
    every { stateApiClient.createOrUpdateState(any()) } throws IOException()
    actualFlushMethod.captured.run()

    val stateB1 = getStreamState("closeB", 1)
    syncPersistence.accept(connectionId, stateB1)

    // Final flush
    clearInvocations(listOf(stateApiClient))
    every { stateApiClient.createOrUpdateState(any()) } returns mockk(relaxed = true)
    every { executorService.awaitTermination(any(), any()) } returns true
    syncPersistence.close()
    verifyStateUpdateApiCall(listOf(stateA2, stateB1))
  }

  @Test
  @Throws(Exception::class)
  fun testCloseShouldAttemptToRetryFinalFlush() {
    val state = getStreamState("final retry", 2)
    syncPersistence.updateStats(AirbyteRecordMessage())
    syncPersistence.accept(connectionId, state)

    every { stateApiClient.createOrUpdateState(any()) } returns mockk(relaxed = true)

    // Final flush
    every { executorService.awaitTermination(any(), any()) } returns true
    syncPersistence.close()
    verify(exactly = 1) { stateApiClient.createOrUpdateState(buildStateRequest(connectionId = connectionId, listOf(state))) }
    verify(exactly = 1) { attemptApiClient.saveStats(any()) }
  }

  @Test
  @Throws(IOException::class, InterruptedException::class)
  fun testBadFinalStateFlushThrowsAnException() {
    val state = getStreamState("final retry", 2)
    syncPersistence.updateStats(AirbyteRecordMessage())
    syncPersistence.accept(connectionId, state)

    // Setup some API failures
    every { stateApiClient.createOrUpdateState(any()) } throws IOException()

    // Final flush
    every { executorService.awaitTermination(any(), any()) } returns true
    assertThrows(Exception::class.java) { syncPersistence.close() }

    verify(exactly = 1) { stateApiClient.createOrUpdateState(buildStateRequest(connectionId = connectionId, listOf(state))) }
    verify(exactly = 0) { attemptApiClient.saveStats(any()) }

    // Reset the mock so that the @AfterEach call to close the syncPersistence object doesn't throw on error
    every { stateApiClient.createOrUpdateState(any()) } returns mockk(relaxed = true)
  }

  @Test
  @Throws(IOException::class, InterruptedException::class)
  fun testBadFinalStatsFlushThrowsAnException() {
    val state = getStreamState("final retry", 2)
    syncPersistence.updateStats(AirbyteRecordMessage())
    syncPersistence.accept(connectionId, state)

    // Setup some API failures
    every { attemptApiClient.saveStats(any()) } throws IOException()

    // Final flush
    every { executorService.awaitTermination(any(), any()) } returns true
    assertThrows(Exception::class.java) { syncPersistence.close() }

    verify(exactly = 1) { stateApiClient.createOrUpdateState(buildStateRequest(connectionId = connectionId, listOf(state))) }
    verify(exactly = 1) { attemptApiClient.saveStats(any()) }

    // Reset the mock so that the @AfterEach call to close the syncPersistence object doesn't throw on error
    every { attemptApiClient.saveStats(any()) } returns mockk(relaxed = true)
  }

  @Test
  @Throws(Exception::class)
  fun testCloseWhenFailBecauseFlushTookTooLong() {
    syncPersistence.accept(connectionId, getStreamState("oops", 42))

    // Simulates a flush taking too long to terminate
    every { executorService.awaitTermination(any(), any()) } returns false
    syncPersistence.close()

    verify(exactly = 1) { executorService.shutdown() }
    // Since the previous write has an unknown state, we do not attempt to persist after the close
    verify(exactly = 0) { stateApiClient.createOrUpdateState(any()) }
  }

  @Test
  @Throws(Exception::class)
  fun testCloseWhenFailBecauseThreadInterrupted() {
    syncPersistence.accept(connectionId, getStreamState("oops", 42))

    // Simulates a flush taking too long to terminate
    every { executorService.awaitTermination(any(), any()) } throws InterruptedException()
    syncPersistence.close()

    verify(exactly = 1) { executorService.shutdown() }
    // Since the previous write has an unknown state, we do not attempt to persist after the close
    verify(exactly = 0) { stateApiClient.createOrUpdateState(any()) }
  }

  @Test
  @Throws(Exception::class)
  fun testCloseWithPendingFlushShouldCallTheApi() {
    // Shutdown, we expect the executor service to be stopped and an stateApiClient to be called
    every { executorService.awaitTermination(any(), any()) } returns true

    syncPersistence.close()
    verify(exactly = 1) { executorService.shutdown() }
    verify(exactly = 0) { stateApiClient.createOrUpdateState(any()) }
  }

  @Test
  fun testPreventMixingDataFromDifferentConnections() {
    val message = getStreamState("stream", 5)
    syncPersistence.accept(connectionId, message)

    assertThrows(IllegalArgumentException::class.java) { syncPersistence.accept(UUID.randomUUID(), message) }
  }

  @Test
  @Throws(Exception::class)
  fun testLegacyStatesAreGettingIntoTheScheduledFlushLogic() {
    val captor = CapturingSlot<ConnectionStateCreateOrUpdate>()

    val message = getLegacyState("myFirstState")
    syncPersistence.accept(connectionId, message)

    verify(exactly = 1) { executorService.scheduleAtFixedRate(any(), any(), any(), any()) }

    actualFlushMethod.captured.run()
    verify(exactly = 1) { stateApiClient.createOrUpdateState(capture(captor)) }
    assertTrue(Jsons.serialize(captor.captured).contains("myFirstState"))
    clearInvocations(listOf(stateApiClient))

    val otherMessage1 = getLegacyState("myOtherState1")
    val otherMessage2 = getLegacyState("myOtherState2")
    syncPersistence.accept(connectionId, otherMessage1)
    syncPersistence.accept(connectionId, otherMessage2)
    every { executorService.awaitTermination(any(), any()) } returns true
    syncPersistence.close()
    verify(exactly = 1) { stateApiClient.createOrUpdateState(capture(captor)) }
    assertTrue(Jsons.serialize(captor.captured).contains("myOtherState2"))
  }

  @Test
  fun testSyncStatsTrackerWrapping() {
    syncStatsTracker = mockk(relaxed = true)

    syncPersistence =
      SyncPersistenceImpl(
        airbyteApiClient = airbyteApiClient,
        stateBuffer = stateAggregator,
        stateFlushExecutorService = executorService,
        stateFlushPeriodInSeconds = FLUSH_PERIOD,
        metricClient = metricClient,
        syncStatsTracker = syncStatsTracker,
        connectionId = connectionId,
        jobId = jobId,
        attemptNumber = attemptNumber,
      )

    syncPersistence.updateStats(AirbyteRecordMessage())
    verify(exactly = 1) { syncStatsTracker.updateStats(any()) }

    syncPersistence.updateEstimates(AirbyteEstimateTraceMessage())
    verify(exactly = 1) { syncStatsTracker.updateEstimates(any()) }
    clearInvocations(listOf(syncStatsTracker))

    syncPersistence.updateDestinationStateStats(AirbyteStateMessage())
    verify(exactly = 1) { syncStatsTracker.updateDestinationStateStats(any()) }
    clearInvocations(listOf(syncStatsTracker))

    syncPersistence.updateSourceStatesStats(AirbyteStateMessage())
    verify(exactly = 1) { syncStatsTracker.updateSourceStatesStats(any()) }
    clearInvocations(listOf(syncStatsTracker))

    syncPersistence.getTotalSourceStateMessagesEmitted()
    verify(exactly = 1) { syncStatsTracker.getTotalSourceStateMessagesEmitted() }
    clearInvocations(listOf(syncStatsTracker))

    syncPersistence.getTotalDestinationStateMessagesEmitted()
    verify(exactly = 1) { syncStatsTracker.getTotalDestinationStateMessagesEmitted() }
    clearInvocations(listOf(syncStatsTracker))

    syncPersistence.getMaxSecondsToReceiveSourceStateMessage()
    verify(exactly = 1) { syncStatsTracker.getMaxSecondsToReceiveSourceStateMessage() }
    clearInvocations(listOf(syncStatsTracker))

    syncPersistence.getMeanSecondsToReceiveSourceStateMessage()
    verify(exactly = 1) { syncStatsTracker.getMeanSecondsToReceiveSourceStateMessage() }
    clearInvocations(listOf(syncStatsTracker))

    syncPersistence.getMaxSecondsBetweenStateMessageEmittedAndCommitted()
    verify(exactly = 1) { syncStatsTracker.getMaxSecondsBetweenStateMessageEmittedAndCommitted() }
    clearInvocations(listOf(syncStatsTracker))

    syncPersistence.getMeanSecondsBetweenStateMessageEmittedAndCommitted()
    verify(exactly = 1) { syncStatsTracker.getMeanSecondsBetweenStateMessageEmittedAndCommitted() }
    clearInvocations(listOf(syncStatsTracker))

    syncPersistence.getUnreliableStateTimingMetrics()
    verify(exactly = 1) { syncStatsTracker.getUnreliableStateTimingMetrics() }
    clearInvocations(listOf(syncStatsTracker))
  }

  private fun verifyStateUpdateApiCall(expectedStateMessages: List<AirbyteStateMessage>) {
    // Using an ArgumentCaptor because we do not have an ordering constraint on the states, so we need
    // to add an unordered collection equals
    val captor = CapturingSlot<ConnectionStateCreateOrUpdate>()

    try {
      verify(exactly = 1) { stateApiClient.createOrUpdateState(capture(captor)) }
    } catch (e: IOException) {
      throw RuntimeException(e)
    }
    val actual = captor.captured
    val expected = buildStateRequest(connectionId, expectedStateMessages)

    // Checking the stream states
    assertEquals(actual.connectionState.streamState, expected.connectionState.streamState)

    // Checking the rest of the payload
    assertEquals(expected.connectionState.connectionId, actual.connectionState.connectionId)
    assertEquals(expected.connectionState.state, actual.connectionState.state)
    assertEquals(expected.connectionState.globalState, actual.connectionState.globalState)
    assertEquals(expected.connectionState.stateType, actual.connectionState.stateType)
    assertEquals(expected.connectionId, actual.connectionId)
  }

  private fun buildStateRequest(
    connectionId: UUID,
    stateMessages: List<AirbyteStateMessage>,
  ): ConnectionStateCreateOrUpdate =
    ConnectionStateCreateOrUpdate(
      connectionId,
      ConnectionState(
        ConnectionStateType.STREAM,
        connectionId,
        null,
        stateMessages.map { s: AirbyteStateMessage ->
          StreamState(
            StreamDescriptor(
              s.stream.streamDescriptor.name,
              s.stream.streamDescriptor.namespace,
            ),
            s.stream.streamState,
          )
        },
        null,
      ),
    )

  private fun getStreamState(
    streamName: String?,
    stateValue: Int,
  ): AirbyteStateMessage =
    AirbyteStateMessage()
      .withType(AirbyteStateMessage.AirbyteStateType.STREAM)
      .withStream(
        AirbyteStreamState()
          .withStreamDescriptor(
            io.airbyte.protocol.models.v0
              .StreamDescriptor()
              .withName(streamName),
          ).withStreamState(Jsons.jsonNode(stateValue)),
      )

  private fun getLegacyState(stateValue: String?): AirbyteStateMessage =
    AirbyteStateMessage()
      .withType(AirbyteStateMessage.AirbyteStateType.LEGACY)
      .withData(Jsons.deserialize("{\"state\":\"$stateValue\"}"))

  private fun clearInvocations(mocks: List<Any>) {
    clearMocks(
      firstMock = mocks.firstOrNull() ?: return,
      mocks = if (mocks.size > 1) mocks.subList(1, mocks.size).toTypedArray() else arrayOf(),
      answers = false,
      recordedCalls = true,
      childMocks = false,
      verificationMarks = true, // resets verify all checks
      exclusionRules = false,
    )
  }
}
