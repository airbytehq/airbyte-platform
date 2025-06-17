/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.bookkeeping

import io.airbyte.analytics.DeploymentFetcher
import io.airbyte.analytics.TrackingIdentity
import io.airbyte.analytics.TrackingIdentityFetcher
import io.airbyte.api.client.model.generated.DeploymentMetadataRead
import io.airbyte.commons.json.Jsons
import io.airbyte.config.StreamSyncStats
import io.airbyte.config.SyncStats
import io.airbyte.container.orchestrator.worker.context.ReplicationInputFeatureFlagReader
import io.airbyte.container.orchestrator.worker.exception.InvalidChecksumException
import io.airbyte.container.orchestrator.worker.model.attachIdToStateMessageFromSource
import io.airbyte.featureflag.EmitStateStatsToSegment
import io.airbyte.featureflag.FailSyncOnInvalidChecksum
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.LogStateMsgs
import io.airbyte.featureflag.TestClient
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.protocol.models.v0.AirbyteEstimateTraceMessage
import io.airbyte.protocol.models.v0.AirbyteGlobalState
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteRecordMessage
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStateStats
import io.airbyte.protocol.models.v0.AirbyteStreamNameNamespacePair
import io.airbyte.protocol.models.v0.AirbyteStreamState
import io.airbyte.protocol.models.v0.StreamDescriptor
import io.airbyte.workers.models.ArchitectureConstants
import io.airbyte.workers.testutils.AirbyteMessageUtils
import io.github.oshai.kotlinlogging.KotlinLogging
import io.mockk.Runs
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import java.util.UUID

private val logger = KotlinLogging.logger { }

class ParallelStreamStatsTrackerTest {
  companion object {
    const val FIELD_NAME = "field"
    const val STREAM1_NAME = "stream1"
    const val STREAM2_NAME = "stream2"

    // This is based of the current size of a record from createRecord
    const val MESSAGE_SIZE = 16L

    val CONNECTION_ID: UUID = UUID.randomUUID()
    val WORKSPACE_ID: UUID = UUID.randomUUID()
    const val JOB_ID: Long = 123L
    const val ATTEMPT_NUMBER: Int = 0
  }

  private val stream1 = AirbyteStreamNameNamespacePair(STREAM1_NAME, null)
  private val stream2 = AirbyteStreamNameNamespacePair(STREAM2_NAME, null)

  private val stream1Message1 = createRecord(STREAM1_NAME, "s1m1")
  private val stream1Message2 = createRecord(STREAM1_NAME, "s1m2")
  private val stream1Message3 = createRecord(STREAM1_NAME, "s1m3")
  private val stream2Message1 = createRecord(STREAM2_NAME, "s2m1")
  private val stream2Message2 = createRecord(STREAM2_NAME, "s2m2")
  private val stream2Message3 = createRecord(STREAM2_NAME, "s2m3")

  private lateinit var metricClient: MetricClient
  private lateinit var checkSumCountEventHandler: StateCheckSumCountEventHandler
  private lateinit var featureFlagClient: FeatureFlagClient
  private lateinit var statsTracker: ParallelStreamStatsTracker
  private lateinit var replicationInput: ReplicationInput

  @BeforeEach
  fun beforeEach() {
    val trackingIdentityFetcher = mockk<TrackingIdentityFetcher>()
    val stateCheckSumErrorReporter = mockk<StateCheckSumErrorReporter>()
    every { stateCheckSumErrorReporter.reportError(any(), any(), any(), any(), any(), any(), any(), any(), any()) } just Runs
    val trackingIdentity = mockk<TrackingIdentity>()
    every { trackingIdentity.email } returns "test"
    every { trackingIdentityFetcher.apply(any(), any()) }.returns(trackingIdentity)
    metricClient = mockk<MetricClient>(relaxed = true)
    featureFlagClient = TestClient(mapOf(EmitStateStatsToSegment.key to true, LogStateMsgs.key to false))
    replicationInput = mockk(relaxed = true)
    checkSumCountEventHandler =
      StateCheckSumCountEventHandler(
        pubSubWriter = null,
        featureFlagClient = featureFlagClient,
        deploymentFetcher = DeploymentFetcher { DeploymentMetadataRead(UUID.randomUUID(), "test", "test") },
        trackingIdentityFetcher = trackingIdentityFetcher,
        stateCheckSumReporter = stateCheckSumErrorReporter,
        connectionId = CONNECTION_ID,
        workspaceId = WORKSPACE_ID,
        jobId = JOB_ID,
        attemptNumber = ATTEMPT_NUMBER,
        epochMilliSupplier = { System.currentTimeMillis() },
        idSupplier = { UUID.randomUUID() },
        platformMode = ArchitectureConstants.ORCHESTRATOR,
        metricClient = metricClient,
        replicationInput = replicationInput,
      )
    statsTracker = ParallelStreamStatsTracker(metricClient, checkSumCountEventHandler, platformMode = ArchitectureConstants.ORCHESTRATOR)
  }

  @Test
  fun testSerialStreamStatsTracking() {
    statsTracker.updateStats(stream1Message1)
    statsTracker.updateStats(stream1Message2)
    val s1State1 = createStreamState(STREAM1_NAME, 2)
    statsTracker.updateSourceStatesStats(s1State1)
    statsTracker.updateDestinationStateStats(s1State1)
    statsTracker.updateStats(stream1Message3)

    statsTracker.updateStats(stream2Message1)
    statsTracker.updateStats(stream2Message2)
    statsTracker.updateStats(stream2Message3)

    val actualSyncStats = statsTracker.getTotalStats(false)
    val actualStreamSyncStats = statsTracker.getAllStreamSyncStats(false)

    val expectedSyncStats = buildSyncStats(6L, 2L)
    assertSyncStatsCoreStatsEquals(expectedSyncStats, actualSyncStats)

    val expectedStreamSyncStats =
      listOf(
        StreamSyncStats()
          .withStreamName(STREAM1_NAME)
          .withStats(buildSyncStats(3L, 2L)),
        StreamSyncStats()
          .withStreamName(STREAM2_NAME)
          .withStats(buildSyncStats(3L, 0L)),
      )
    assertStreamSyncStatsCoreStatsEquals(expectedStreamSyncStats, actualStreamSyncStats)
  }

  @Test
  fun testSerialStreamStatsTrackingOnSingleStream() {
    val s1State1 = createStreamState(STREAM1_NAME, 1)
    val s1State2 = createStreamState(STREAM1_NAME, 2)
    val s1State3 = createStreamState(STREAM1_NAME, 3)

    statsTracker.updateStats(stream1Message1)
    statsTracker.updateSourceStatesStats(s1State1)
    statsTracker.updateStats(stream1Message1)
    statsTracker.updateSourceStatesStats(s1State2)
    statsTracker.updateStats(stream1Message1)
    statsTracker.updateSourceStatesStats(s1State3)

    statsTracker.updateDestinationStateStats(s1State1)
    val actualSyncStatsAfter1 = statsTracker.getTotalStats(false)
    assertSyncStatsCoreStatsEquals(buildSyncStats(3L, 1L), actualSyncStatsAfter1)

    statsTracker.updateDestinationStateStats(s1State2)
    val actualSyncStatsAfter2 = statsTracker.getTotalStats(false)
    assertSyncStatsCoreStatsEquals(buildSyncStats(3L, 2L), actualSyncStatsAfter2)

    statsTracker.updateDestinationStateStats(s1State3)
    val actualSyncStatsAfter3 = statsTracker.getTotalStats(false)
    assertSyncStatsCoreStatsEquals(buildSyncStats(3L, 3L), actualSyncStatsAfter3)
  }

  @Test
  fun testSerialStreamStatsTrackingOnSingleStreamWhileSkippingStates() {
    val s1State1 = createStreamState(STREAM1_NAME, 1)
    val s1State2 = createStreamState(STREAM1_NAME, 2)
    val s1State3 = createStreamState(STREAM1_NAME, 3)
    val s1State4 = createStreamState(STREAM1_NAME, 4)

    statsTracker.updateStats(stream1Message1)
    statsTracker.updateSourceStatesStats(s1State1)
    statsTracker.updateStats(stream1Message1)
    statsTracker.updateStats(stream1Message1)
    statsTracker.updateSourceStatesStats(s1State2)
    statsTracker.updateStats(stream1Message1)
    statsTracker.updateSourceStatesStats(s1State3)
    statsTracker.updateStats(stream1Message1)

    statsTracker.updateDestinationStateStats(s1State2)
    val actualSyncStatsAfter1 = statsTracker.getTotalStats(false)
    assertSyncStatsCoreStatsEquals(buildSyncStats(5L, 3L), actualSyncStatsAfter1)

    // Adding more messages around the state to also test the emitted tracking logic
    statsTracker.updateStats(stream1Message1)
    statsTracker.updateSourceStatesStats(s1State4)
    statsTracker.updateStats(stream1Message1)
    statsTracker.updateStats(stream1Message1)

    statsTracker.updateDestinationStateStats(s1State4)
    val actualSyncStatsAfter2 = statsTracker.getTotalStats(false)
    assertSyncStatsCoreStatsEquals(buildSyncStats(8L, 6L), actualSyncStatsAfter2)
  }

  @Test
  fun testSerialStreamStatsTrackingCompletedSync() {
    statsTracker.updateStats(stream1Message1)
    val s1State1 = createStreamState(STREAM1_NAME, 1)
    statsTracker.updateSourceStatesStats(s1State1)
    statsTracker.updateDestinationStateStats(s1State1)

    statsTracker.updateStats(stream2Message1)
    statsTracker.updateStats(stream2Message2)
    statsTracker.updateStats(stream2Message3)
    val s2State1 = createStreamState(STREAM2_NAME, 3)
    statsTracker.updateSourceStatesStats(s2State1)
    statsTracker.updateDestinationStateStats(s2State1)

    // Worth noting, in the current implementation, if replication has completed, we assume all records
    // to be committed, even though there is no state messages after.
    statsTracker.updateStats(stream1Message2)

    val actualSyncStats = statsTracker.getTotalStats(true)
    val actualStreamSyncStats = statsTracker.getAllStreamSyncStats(true)

    val expectedSyncStats = buildSyncStats(5L, 5L)
    assertSyncStatsCoreStatsEquals(expectedSyncStats, actualSyncStats)

    val expectedStreamSyncStats =
      listOf(
        StreamSyncStats()
          .withStreamName(STREAM1_NAME)
          .withStats(buildSyncStats(2L, 2L)),
        StreamSyncStats()
          .withStreamName(STREAM2_NAME)
          .withStats(buildSyncStats(3L, 3L)),
      )
    assertStreamSyncStatsCoreStatsEquals(expectedStreamSyncStats, actualStreamSyncStats)
  }

  @Test
  fun testParallelStreamStatsTracking() {
    statsTracker.updateStats(stream1Message1)
    statsTracker.updateStats(stream2Message1)
    statsTracker.updateStats(stream1Message2)
    val s1State1 = createStreamState(STREAM1_NAME, 2)
    statsTracker.updateSourceStatesStats(s1State1)
    statsTracker.updateStats(stream2Message2)
    statsTracker.updateStats(stream1Message3)
    val s1State2 = createStreamState(STREAM1_NAME, 3)
    statsTracker.updateSourceStatesStats(s1State2)
    statsTracker.updateDestinationStateStats(s1State1)

    // At this point, only s1state1 has been committed.
    val midSyncCheckpoint1Stats = statsTracker.getTotalStats(false)
    val expectedMidSyncCheckpoint1Stats = buildSyncStats(5L, 2L)
    assertSyncStatsCoreStatsEquals(expectedMidSyncCheckpoint1Stats, midSyncCheckpoint1Stats)

    // Sending more state for stream 2
    val s2State1 = createStreamState(STREAM2_NAME, 2)
    statsTracker.updateSourceStatesStats(s2State1)
    statsTracker.updateDestinationStateStats(s2State1)

    // We should now have data for stream two as well
    val midSyncCheckpoint2Stats = statsTracker.getTotalStats(false)
    val expectedMidSyncCheckpoint2Stats = buildSyncStats(5L, 4L)
    assertSyncStatsCoreStatsEquals(expectedMidSyncCheckpoint2Stats, midSyncCheckpoint2Stats)

    // Closing up states
    statsTracker.updateDestinationStateStats(s1State2)
    val midSyncCheckpoint3Stats = statsTracker.getTotalStats(false)
    val expectedMidSyncCheckpoint3Stats = buildSyncStats(5L, 5L)
    assertSyncStatsCoreStatsEquals(expectedMidSyncCheckpoint3Stats, midSyncCheckpoint3Stats)
  }

  @Test
  fun testCommittedStatsTrackingWithGlobalStates() {
    // emitted records

    statsTracker.updateStats(stream1Message1)
    statsTracker.updateStats(stream2Message1)
    statsTracker.updateStats(stream1Message2)
    val globalState1 = createGlobalState(1, STREAM1_NAME, STREAM2_NAME)
    // emitted records so far paired with globalState1
    statsTracker.updateSourceStatesStats(globalState1)

    // emitted records that will never be committed
    statsTracker.updateStats(stream2Message2)
    statsTracker.updateStats(stream1Message3)

    val globalState2 = createGlobalState(2, STREAM1_NAME, STREAM2_NAME)
    // the last 2 emitted records paired with globalState2
    statsTracker.updateSourceStatesStats(globalState2)

    // records paired with globalState1 are now considered committed
    statsTracker.updateDestinationStateStats(globalState1)

    val streamToCommittedRecords = statsTracker.getStreamToCommittedRecords()

    assertEquals(2, streamToCommittedRecords.size)
    assertEquals(2, streamToCommittedRecords[stream1])
    assertEquals(1, streamToCommittedRecords[stream2])
    assertEquals(3, statsTracker.getTotalRecordsCommitted())
  }

  @Test
  fun testDuplicatedSourceStates() {
    val s1State1 = createStreamState(STREAM1_NAME, 1)
    val s1State2 = createStreamState(STREAM1_NAME, 2)
    val s2State1 = createStreamState(STREAM2_NAME, 1)
    statsTracker.updateStats(stream1Message1)
    statsTracker.updateSourceStatesStats(s1State1)
    statsTracker.updateDestinationStateStats(s1State1)
    statsTracker.updateStats(stream1Message2)
    statsTracker.updateSourceStatesStats(s1State2)
    statsTracker.updateSourceStatesStats(s1State2) // We will drop mid-sync committed stats for the stream because of this
    statsTracker.updateDestinationStateStats(s1State2)
    statsTracker.updateStats(stream2Message1)
    statsTracker.updateSourceStatesStats(s2State1)
    statsTracker.updateDestinationStateStats(s2State1)

    val actualMidSyncSyncStats = statsTracker.getTotalStats(false)
    assertSyncStatsCoreStatsEquals(buildSyncStats(3L, 2L), actualMidSyncSyncStats)

    // hasReplicationCompleted: true, means the sync was successful, we still expect committed to equal
    // emitted in this case.
    val actualSyncStats = statsTracker.getTotalStats(true)
    assertSyncStatsCoreStatsEquals(buildSyncStats(3L, 3L), actualSyncStats)

    verify(exactly = 1) { metricClient.count(metric = OssMetricsRegistry.STATE_ERROR_COLLISION_FROM_SOURCE, value = 1) }

    // The following metrics are expected to be discarded
    assertNull(statsTracker.getMaxSecondsBetweenStateMessageEmittedAndCommitted())
    assertNull(statsTracker.getMeanSecondsBetweenStateMessageEmittedAndCommitted())
  }

  @Test
  fun testUnexpectedStateFromDestination() {
    val s1State1 = createStreamState(STREAM1_NAME, 1)
    val s1State2 = createStreamState(STREAM1_NAME, 2)

    statsTracker.updateStats(stream1Message1)
    statsTracker.updateSourceStatesStats(s1State1)
    statsTracker.updateDestinationStateStats(
      createStreamState(
        STREAM1_NAME,
        5,
      ),
    ) // This is unexpected since it never came from the source.
    statsTracker.updateDestinationStateStats(s1State1)
    statsTracker.updateStats(stream1Message2)
    statsTracker.updateSourceStatesStats(s1State2)
    statsTracker.updateDestinationStateStats(s1State2)

    val actualMidSyncSyncStats = statsTracker.getTotalStats(false)
    assertSyncStatsCoreStatsEquals(buildSyncStats(2L, 2L), actualMidSyncSyncStats)

    verify(exactly = 1) { metricClient.count(metric = OssMetricsRegistry.STATE_ERROR_UNKNOWN_FROM_DESTINATION, value = 1) }
  }

  @Test
  fun testReceivingTheSameStateFromDestinationDoesntFlushUnexpectedStates() {
    val s1State1 = createStreamState(STREAM1_NAME, 1)
    val s1State2 = createStreamState(STREAM1_NAME, 2)
    val s1State3 = createStreamState(STREAM1_NAME, 3)

    statsTracker.updateStats(stream1Message1)
    statsTracker.updateSourceStatesStats(s1State1)
    statsTracker.updateStats(stream1Message1)
    statsTracker.updateSourceStatesStats(s1State2)
    statsTracker.updateStats(stream1Message1)
    statsTracker.updateSourceStatesStats(s1State3)

    // Sending state 2 should clear state1 and state2
    statsTracker.updateDestinationStateStats(s1State2)
    val statsAfterState2 = statsTracker.getTotalStats(false)
    assertSyncStatsCoreStatsEquals(buildSyncStats(3L, 2L), statsAfterState2)

    // Sending state 1 out of order
    statsTracker.updateDestinationStateStats(s1State1)
    val statsAfterState1OutOfOrder = statsTracker.getTotalStats(false)
    // Stats count should remain stable because state1 has already been handled
    assertSyncStatsCoreStatsEquals(buildSyncStats(3L, 2L), statsAfterState1OutOfOrder)
    verify(exactly = 1) { metricClient.count(metric = OssMetricsRegistry.STATE_ERROR_UNKNOWN_FROM_DESTINATION, value = 1) }

    // Sending state 2 again
    statsTracker.updateDestinationStateStats(s1State2)
    val statsAfterState2Again = statsTracker.getTotalStats(false)
    // Stats count should remain stable because state1 has already been handled
    assertSyncStatsCoreStatsEquals(buildSyncStats(3L, 2L), statsAfterState2Again)
    verify(exactly = 2) { metricClient.count(metric = OssMetricsRegistry.STATE_ERROR_UNKNOWN_FROM_DESTINATION, value = 1) }

    // Sending state 3
    clearMocks(metricClient)
    statsTracker.updateDestinationStateStats(s1State3)
    val statsAfterState3 = statsTracker.getTotalStats(false)
    // Stats count should remain stable because state1 has already been handled
    assertSyncStatsCoreStatsEquals(buildSyncStats(3L, 3L), statsAfterState3)
    verify(exactly = 0) { metricClient.count(metric = OssMetricsRegistry.STATE_ERROR_UNKNOWN_FROM_DESTINATION, value = 1) }

    // Sending state 3 again
    statsTracker.updateDestinationStateStats(s1State3)
    val statsAfterState3Again = statsTracker.getTotalStats(false)
    // Stats count should remain stable because state1 has already been handled
    assertSyncStatsCoreStatsEquals(buildSyncStats(3L, 3L), statsAfterState3Again)
    verify(exactly = 1) { metricClient.count(metric = OssMetricsRegistry.STATE_ERROR_UNKNOWN_FROM_DESTINATION, value = 1) }
  }

  @Test
  fun testAccessors() {
    statsTracker.updateStats(stream2Message1)
    statsTracker.updateStats(stream1Message1)
    statsTracker.updateStats(stream2Message1)
    val s1State1 = createStreamState(STREAM1_NAME, 1)
    val s2State1 = createStreamState(STREAM2_NAME, 2)
    statsTracker.updateSourceStatesStats(s1State1)
    statsTracker.updateSourceStatesStats(s2State1)
    statsTracker.updateDestinationStateStats(s2State1)

    assertEquals(
      mapOf(stream1 to 0L, stream2 to 2L),
      statsTracker.getStreamToCommittedRecords(),
    )
    assertEquals(
      mapOf(stream1 to 0L, stream2 to 2L * MESSAGE_SIZE),
      statsTracker.getStreamToCommittedBytes(),
    )

    assertEquals(mapOf(stream1 to 1L, stream2 to 2L), statsTracker.getStreamToEmittedRecords())
    assertEquals(
      mapOf(stream1 to 1L * MESSAGE_SIZE, stream2 to 2L * MESSAGE_SIZE),
      statsTracker.getStreamToEmittedBytes(),
    )

    assertEquals(3L, statsTracker.getTotalRecordsEmitted())
    assertEquals(3L * MESSAGE_SIZE, statsTracker.getTotalBytesEmitted())
    assertEquals(2L, statsTracker.getTotalRecordsCommitted())
    assertEquals(2L * MESSAGE_SIZE, statsTracker.getTotalBytesCommitted())
  }

  @Test
  fun testGettersDontCrashWhenThereIsNoData() {
    // Looking for null pointers so no exceptions means all good for most part.
    statsTracker.getTotalStats(false)
    statsTracker.getTotalStats(true)

    assertEquals(0L, statsTracker.getTotalBytesEmitted())
    assertEquals(0L, statsTracker.getTotalRecordsEmitted())
    assertNull(statsTracker.getTotalBytesCommitted())
    assertNull(statsTracker.getTotalRecordsCommitted())
    assertEquals(0L, statsTracker.getTotalBytesEstimated())
    assertEquals(0L, statsTracker.getTotalRecordsEstimated())
  }

  @Test
  fun testStreamEstimates() {
    val estimateStream1Message1 = createEstimate(STREAM1_NAME, 1L, 1L)
    val estimateStream1Message2 = createEstimate(STREAM1_NAME, 10L, 2L)
    val estimateStream2 = createEstimate(STREAM2_NAME, 100L, 21L)

    // Note that estimates are global, we override the count for each message rather than sum
    statsTracker.updateEstimates(estimateStream1Message1)
    statsTracker.updateEstimates(estimateStream1Message2)
    statsTracker.updateEstimates(estimateStream2)

    val actualSyncStats = statsTracker.getTotalStats(false)
    assertEquals(
      buildSyncStats(0L, 0L).withEstimatedBytes(110L).withEstimatedRecords(23L),
      actualSyncStats,
    )

    val actualStreamSyncStats = statsTracker.getAllStreamSyncStats(false)
    val expectedStreamSyncStats =
      listOf(
        StreamSyncStats()
          .withStreamName(STREAM1_NAME)
          .withStats(buildSyncStats(0L, 0L).withEstimatedBytes(10L).withEstimatedRecords(2L)),
        StreamSyncStats()
          .withStreamName(STREAM2_NAME)
          .withStats(buildSyncStats(0L, 0L).withEstimatedBytes(100L).withEstimatedRecords(21L)),
      )
    assertStreamSyncStatsCoreStatsEquals(expectedStreamSyncStats, actualStreamSyncStats)

    assertEquals(23L, statsTracker.getTotalRecordsEstimated())
    assertEquals(110L, statsTracker.getTotalBytesEstimated())
    assertEquals(
      mapOf(stream1 to 2L, stream2 to 21L),
      statsTracker.getStreamToEstimatedRecords(),
    )
    assertEquals(
      mapOf(stream1 to 10L, stream2 to 100L),
      statsTracker.getStreamToEstimatedBytes(),
    )
  }

  @Test
  fun testSyncEstimates() {
    val syncEstimate1 = createSyncEstimate(2L, 1L)
    val syncEstimate2 = createSyncEstimate(15L, 5L)
    statsTracker.updateEstimates(syncEstimate1)
    statsTracker.updateEstimates(syncEstimate2)

    val actualSyncStats = statsTracker.getTotalStats(false)
    assertEquals(SyncStats().withEstimatedBytes(15L).withEstimatedRecords(5L), actualSyncStats)
    assertEquals(listOf<Any>(), statsTracker.getAllStreamSyncStats(false))

    assertEquals(5L, statsTracker.getTotalRecordsEstimated())
    assertEquals(15L, statsTracker.getTotalBytesEstimated())
    assertEquals(mapOf<Any, Any>(), statsTracker.getStreamToEstimatedRecords())
    assertEquals(mapOf<Any, Any>(), statsTracker.getStreamToEstimatedBytes())
  }

  @Test
  fun testEstimateTypeConflicts() {
    statsTracker.updateEstimates(createEstimate(STREAM2_NAME, 4L, 2L))
    statsTracker.updateEstimates(createSyncEstimate(3L, 1L))

    val actualSyncStats = statsTracker.getTotalStats(false)
    assertEquals(buildSyncStats(0L, 0L), actualSyncStats)
    assertEquals(0L, statsTracker.getTotalRecordsEstimated())
    assertEquals(0L, statsTracker.getTotalBytesEstimated())
    assertEquals(mapOf<Any, Any>(), statsTracker.getStreamToEstimatedBytes())
    assertEquals(mapOf<Any, Any>(), statsTracker.getStreamToEstimatedRecords())
  }

  @Test
  @Throws(InterruptedException::class)
  fun testCheckpointingMetrics() {
    val s1State1 = createStreamState(STREAM1_NAME, 1)
    val s1State2 = createStreamState(STREAM1_NAME, 2)
    val s2State1 = createStreamState(STREAM2_NAME, 1)
    val s2State2 = createStreamState(STREAM2_NAME, 3)

    statsTracker.updateSourceStatesStats(s1State1)
    statsTracker.updateSourceStatesStats(s2State1)
    Thread.sleep(1000)
    statsTracker.updateSourceStatesStats(s1State2)
    Thread.sleep(1000)
    statsTracker.updateSourceStatesStats(s2State2)
    statsTracker.updateDestinationStateStats(s1State1)
    statsTracker.updateDestinationStateStats(s2State1)

    assertEquals(4, statsTracker.getTotalSourceStateMessagesEmitted())
    assertEquals(2, statsTracker.getTotalDestinationStateMessagesEmitted())

    // We only check for a non-zero value for sanity check to avoid jitter from time.
    assertTrue(statsTracker.getMaxSecondsToReceiveSourceStateMessage() > 0)
    assertTrue(statsTracker.getMeanSecondsToReceiveSourceStateMessage() > 0)
    assertTrue(statsTracker.getMaxSecondsBetweenStateMessageEmittedAndCommitted()!! > 0)
    assertTrue(statsTracker.getMeanSecondsBetweenStateMessageEmittedAndCommitted()!! > 0)
  }

  @Test
  fun testGetMeanSecondsToReceiveSourceStateMessageReturnsZeroWhenEmpty() {
    assertEquals(0, statsTracker.getMeanSecondsToReceiveSourceStateMessage())
  }

  @Test
  fun testGetMaxSecondsToReceiveSourceStateMessageReturnsZeroWhenEmpty() {
    assertEquals(0, statsTracker.getMaxSecondsToReceiveSourceStateMessage())
  }

  @Test
  fun testGetMaxSecondsBetweenStateMessageEmittedAndCommittedReturnsNullWhenEmpty() {
    assertNull(statsTracker.getMaxSecondsBetweenStateMessageEmittedAndCommitted())
  }

  @Test
  fun testNoStatsForNullStreamAreReturned() {
    // Checking for LegacyStates
    val legacyState = attachIdToStateMessageFromSource(AirbyteMessageUtils.createStateMessage(1337)).state

    statsTracker.updateSourceStatesStats(legacyState)
    statsTracker.updateDestinationStateStats(legacyState)

    val actualLegacyStreamStats = statsTracker.getAllStreamSyncStats(false)
    assertStreamSyncStatsCoreStatsEquals(listOf(), actualLegacyStreamStats)

    assertTrue(statsTracker.getStreamToEmittedRecords().isEmpty())
    assertTrue(statsTracker.getStreamToEmittedBytes().isEmpty())
    assertTrue(statsTracker.getStreamToEstimatedRecords().isEmpty())
    assertTrue(statsTracker.getStreamToEstimatedBytes().isEmpty())
    assertTrue(statsTracker.getStreamToCommittedRecords().isEmpty())
    assertTrue(statsTracker.getStreamToCommittedBytes().isEmpty())

    // Checking for GlobalStates
    val globalState = createGlobalState(1337)

    statsTracker.updateSourceStatesStats(globalState)
    statsTracker.updateDestinationStateStats(globalState)

    val actualGlobalStreamStats = statsTracker.getAllStreamSyncStats(false)
    assertStreamSyncStatsCoreStatsEquals(listOf(), actualGlobalStreamStats)
  }

  @Test
  internal fun `test that an exception is raised when the source stats do not match the tracked emitted stats`() {
    val name = "name"
    val namespace = "namespace"
    val recordCount = 10
    val stateMessage1 =
      attachIdToStateMessageFromSource(
        AirbyteMessage().withType(AirbyteMessage.Type.STATE).withState(
          AirbyteStateMessage()
            .withStream(
              AirbyteStreamState()
                .withStreamDescriptor(StreamDescriptor().withName(name).withNamespace(namespace))
                .withStreamState(Jsons.jsonNode(mapOf("id" to 10))),
            ).withSourceStats(AirbyteStateStats().withRecordCount(recordCount.toDouble()))
            .withType(AirbyteStateMessage.AirbyteStateType.STREAM),
        ),
      )
    val stateMessage2 =
      attachIdToStateMessageFromSource(
        AirbyteMessage().withType(AirbyteMessage.Type.STATE).withState(
          AirbyteStateMessage()
            .withStream(
              AirbyteStreamState()
                .withStreamDescriptor(StreamDescriptor().withName(name).withNamespace(namespace))
                .withStreamState(Jsons.jsonNode(mapOf("id" to 20))),
            ).withSourceStats(AirbyteStateStats().withRecordCount(recordCount.toDouble()))
            .withType(AirbyteStateMessage.AirbyteStateType.STREAM),
        ),
      )

    val replicationInputFeatureFlagReader =
      mockk<ReplicationInputFeatureFlagReader> {
        every { read(FailSyncOnInvalidChecksum) } returns true
        every { read(LogStateMsgs) } returns false
      }
    statsTracker.setReplicationFeatureFlagReader(replicationInputFeatureFlagReader)

    trackRecords(recordCount, name, namespace)

    // First assert that the checksums match
    statsTracker.updateSourceStatesStats(stateMessage1.state)

    trackRecords(recordCount - 2, name, namespace)

    assertThrows(InvalidChecksumException::class.java) {
      statsTracker.updateSourceStatesStats(stateMessage2.state)
    }
  }

  @Test
  internal fun `test that an exception is raised when the destination stats do not match the tracked committed stats`() {
    val name = "name"
    val namespace = "namespace"
    val recordCount = 10
    val stateMessage1 =
      attachIdToStateMessageFromSource(
        AirbyteMessage().withType(AirbyteMessage.Type.STATE).withState(
          AirbyteStateMessage()
            .withStream(
              AirbyteStreamState()
                .withStreamDescriptor(StreamDescriptor().withName(name).withNamespace(namespace))
                .withStreamState(Jsons.jsonNode(mapOf("id" to 10))),
            ).withSourceStats(AirbyteStateStats().withRecordCount(recordCount.toDouble()))
            .withDestinationStats(AirbyteStateStats().withRecordCount(recordCount.toDouble()))
            .withType(AirbyteStateMessage.AirbyteStateType.STREAM),
        ),
      )
    val stateMessage2 =
      attachIdToStateMessageFromSource(
        AirbyteMessage().withType(AirbyteMessage.Type.STATE).withState(
          AirbyteStateMessage()
            .withStream(
              AirbyteStreamState()
                .withStreamDescriptor(StreamDescriptor().withName(name).withNamespace(namespace))
                .withStreamState(Jsons.jsonNode(mapOf("id" to 20))),
            ).withSourceStats(AirbyteStateStats().withRecordCount(recordCount.toDouble() - 2))
            .withDestinationStats(AirbyteStateStats().withRecordCount(recordCount.toDouble()))
            .withType(AirbyteStateMessage.AirbyteStateType.STREAM),
        ),
      )

    val replicationInputFeatureFlagReader =
      mockk<ReplicationInputFeatureFlagReader> {
        every { read(FailSyncOnInvalidChecksum) } returns true
        every { read(LogStateMsgs) } returns false
      }
    statsTracker.setReplicationFeatureFlagReader(replicationInputFeatureFlagReader)

    trackRecords(recordCount, name, namespace)

    // First assert that the checksums match
    statsTracker.updateSourceStatesStats(stateMessage1.state)
    statsTracker.updateDestinationStateStats(stateMessage1.state)

    trackRecords(recordCount - 2, name, namespace)

    statsTracker.updateSourceStatesStats(stateMessage2.state)

    assertThrows(InvalidChecksumException::class.java) {
      statsTracker.updateDestinationStateStats(stateMessage2.state)
    }
  }

  @Test
  internal fun `test that an exception is raised when the source stats do not match the destination stats in the state message`() {
    val name = "name"
    val namespace = "namespace"
    val recordCount = 10
    val stateMessage1 =
      attachIdToStateMessageFromSource(
        AirbyteMessage().withType(AirbyteMessage.Type.STATE).withState(
          AirbyteStateMessage()
            .withStream(
              AirbyteStreamState()
                .withStreamDescriptor(StreamDescriptor().withName(name).withNamespace(namespace))
                .withStreamState(Jsons.jsonNode(mapOf("id" to 10))),
            ).withSourceStats(AirbyteStateStats().withRecordCount(recordCount.toDouble()))
            .withDestinationStats(AirbyteStateStats().withRecordCount(recordCount.toDouble()))
            .withType(AirbyteStateMessage.AirbyteStateType.STREAM),
        ),
      )
    val stateMessage2 =
      attachIdToStateMessageFromSource(
        AirbyteMessage().withType(AirbyteMessage.Type.STATE).withState(
          AirbyteStateMessage()
            .withStream(
              AirbyteStreamState()
                .withStreamDescriptor(StreamDescriptor().withName(name).withNamespace(namespace))
                .withStreamState(Jsons.jsonNode(mapOf("id" to 20))),
            ).withSourceStats(AirbyteStateStats().withRecordCount(recordCount.toDouble()))
            .withDestinationStats(AirbyteStateStats().withRecordCount(recordCount.toDouble()))
            .withType(AirbyteStateMessage.AirbyteStateType.STREAM),
        ),
      )

    val replicationInputFeatureFlagReader =
      mockk<ReplicationInputFeatureFlagReader> {
        every { read(FailSyncOnInvalidChecksum) } returns true
        every { read(LogStateMsgs) } returns false
      }
    statsTracker.setReplicationFeatureFlagReader(replicationInputFeatureFlagReader)

    trackRecords(recordCount, name, namespace)

    // First assert that the checksums match
    statsTracker.updateSourceStatesStats(stateMessage1.state)
    statsTracker.updateDestinationStateStats(stateMessage1.state)

    trackRecords(recordCount, name, namespace)
    statsTracker.updateSourceStatesStats(stateMessage2.state)

    assertThrows(InvalidChecksumException::class.java) {
      stateMessage2.state.sourceStats.recordCount = (recordCount - 2).toDouble()
      statsTracker.updateDestinationStateStats(stateMessage2.state)
    }
  }

  @Test
  internal fun `test that no exception is raised when the state message checksum comparison passes`() {
    val name = "name"
    val namespace = "namespace"
    val recordCount = 10
    val stateMessage1 =
      attachIdToStateMessageFromSource(
        AirbyteMessage().withType(AirbyteMessage.Type.STATE).withState(
          AirbyteStateMessage()
            .withStream(
              AirbyteStreamState()
                .withStreamDescriptor(StreamDescriptor().withName(name).withNamespace(namespace))
                .withStreamState(Jsons.jsonNode(mapOf("id" to 10))),
            ).withSourceStats(AirbyteStateStats().withRecordCount(recordCount.toDouble()))
            .withDestinationStats(AirbyteStateStats().withRecordCount(recordCount.toDouble()))
            .withType(AirbyteStateMessage.AirbyteStateType.STREAM),
        ),
      )
    val stateMessage2 =
      attachIdToStateMessageFromSource(
        AirbyteMessage().withType(AirbyteMessage.Type.STATE).withState(
          AirbyteStateMessage()
            .withStream(
              AirbyteStreamState()
                .withStreamDescriptor(StreamDescriptor().withName(name).withNamespace(namespace))
                .withStreamState(Jsons.jsonNode(mapOf("id" to 20))),
            ).withSourceStats(AirbyteStateStats().withRecordCount(recordCount.toDouble()))
            .withDestinationStats(AirbyteStateStats().withRecordCount(recordCount.toDouble()))
            .withType(AirbyteStateMessage.AirbyteStateType.STREAM),
        ),
      )

    val replicationInputFeatureFlagReader =
      mockk<ReplicationInputFeatureFlagReader> {
        every { read(FailSyncOnInvalidChecksum) } returns true
        every { read(LogStateMsgs) } returns false
      }
    statsTracker.setReplicationFeatureFlagReader(replicationInputFeatureFlagReader)

    assertDoesNotThrow {
      trackRecords(recordCount, name, namespace)
      // First assert that the checksums match
      statsTracker.updateSourceStatesStats(stateMessage1.state)
      statsTracker.updateDestinationStateStats(stateMessage1.state)

      trackRecords(recordCount, name, namespace)
      statsTracker.updateSourceStatesStats(stateMessage2.state)
      statsTracker.updateDestinationStateStats(stateMessage2.state)
    }
  }

  @Test
  internal fun `test that no exception is raised when the state message checksum comparison is disabled due to collisions`() {
    val name = "name"
    val namespace = "namespace"
    val recordCount = 10
    val stateMessage1 =
      attachIdToStateMessageFromSource(
        AirbyteMessage().withType(AirbyteMessage.Type.STATE).withState(
          AirbyteStateMessage()
            .withStream(
              AirbyteStreamState()
                .withStreamDescriptor(StreamDescriptor().withName(name).withNamespace(namespace))
                .withStreamState(Jsons.jsonNode(mapOf("id" to 10))),
            ).withSourceStats(AirbyteStateStats().withRecordCount(recordCount.toDouble()))
            .withDestinationStats(AirbyteStateStats().withRecordCount(recordCount.toDouble()))
            .withType(AirbyteStateMessage.AirbyteStateType.STREAM),
        ),
      )

    val replicationInputFeatureFlagReader =
      mockk<ReplicationInputFeatureFlagReader> {
        every { read(FailSyncOnInvalidChecksum) } returns true
        every { read(LogStateMsgs) } returns false
      }
    statsTracker.setReplicationFeatureFlagReader(replicationInputFeatureFlagReader)

    assertDoesNotThrow {
      trackRecords(recordCount, name, namespace)
      // First assert that the checksums match
      statsTracker.updateSourceStatesStats(stateMessage1.state)

      trackRecords(recordCount, name, namespace)
      statsTracker.updateSourceStatesStats(stateMessage1.state)

      statsTracker.updateDestinationStateStats(stateMessage1.state)
      statsTracker.updateDestinationStateStats(stateMessage1.state)
    }
    assertFalse(statsTracker.isChecksumValidationEnabled())
  }

  @Test
  internal fun `test that hash collision doesnt happen when same state messages arrive`() {
    val name = "name"
    val namespace = "namespace"
    val recordCount = 10
    val stateMessage1 =
      AirbyteStateMessage()
        .withStream(
          AirbyteStreamState()
            .withStreamDescriptor(StreamDescriptor().withName(name).withNamespace(namespace))
            .withStreamState(Jsons.jsonNode(mapOf("id" to 10))),
        ).withSourceStats(AirbyteStateStats().withRecordCount(recordCount.toDouble()))
        .withDestinationStats(AirbyteStateStats().withRecordCount(recordCount.toDouble()))
        .withType(AirbyteStateMessage.AirbyteStateType.STREAM)
    val copyOfStateMessage1 =
      AirbyteStateMessage()
        .withStream(
          AirbyteStreamState()
            .withStreamDescriptor(StreamDescriptor().withName(name).withNamespace(namespace))
            .withStreamState(Jsons.jsonNode(mapOf("id" to 10))),
        ).withSourceStats(AirbyteStateStats().withRecordCount(recordCount.toDouble()))
        .withDestinationStats(AirbyteStateStats().withRecordCount(recordCount.toDouble()))
        .withType(AirbyteStateMessage.AirbyteStateType.STREAM)

    assertEquals(stateMessage1, copyOfStateMessage1)
    val state = attachIdToStateMessageFromSource(AirbyteMessage().withType(AirbyteMessage.Type.STATE).withState(stateMessage1)).state
    val state2 =
      attachIdToStateMessageFromSource(
        AirbyteMessage().withType(AirbyteMessage.Type.STATE).withState(copyOfStateMessage1),
      ).state

    val replicationInputFeatureFlagReader =
      mockk<ReplicationInputFeatureFlagReader> {
        every { read(FailSyncOnInvalidChecksum) } returns true
        every { read(LogStateMsgs) } returns false
      }
    statsTracker.setReplicationFeatureFlagReader(replicationInputFeatureFlagReader)

    assertDoesNotThrow {
      trackRecords(recordCount, name, namespace)
      // First assert that the checksums match
      statsTracker.updateSourceStatesStats(state)

      trackRecords(recordCount, name, namespace)
      statsTracker.updateSourceStatesStats(state2)

      statsTracker.updateDestinationStateStats(state)
      statsTracker.updateDestinationStateStats(state2)
    }
    assertTrue(statsTracker.isChecksumValidationEnabled())
  }

  @Test
  internal fun `test that no exception is raised when the state message checksum comparison passes for global state`() {
    val name1 = "name1"
    val namespace1 = "namespace1"
    val name2 = "name2"
    val namespace2 = "namespace2"
    val recordCountStream1 = 10
    val recordCountStream2 = 15
    val stateMessage1 =
      attachIdToStateMessageFromSource(
        AirbyteMessage().withType(AirbyteMessage.Type.STATE).withState(
          AirbyteStateMessage()
            .withGlobal(
              AirbyteGlobalState()
                .withSharedState(Jsons.jsonNode(mapOf("wal" to 10)))
                .withStreamStates(
                  listOf(
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName(name1).withNamespace(namespace1))
                      .withStreamState(Jsons.jsonNode(mapOf("id" to 10))),
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName(name2).withNamespace(namespace2))
                      .withStreamState(Jsons.jsonNode(mapOf("id" to 15))),
                  ),
                ),
            ).withSourceStats(AirbyteStateStats().withRecordCount((recordCountStream1 + recordCountStream2).toDouble()))
            .withDestinationStats(AirbyteStateStats().withRecordCount((recordCountStream1 + recordCountStream2).toDouble()))
            .withType(AirbyteStateMessage.AirbyteStateType.GLOBAL),
        ),
      )
    val stateMessage2 =
      attachIdToStateMessageFromSource(
        AirbyteMessage().withType(AirbyteMessage.Type.STATE).withState(
          AirbyteStateMessage()
            .withGlobal(
              AirbyteGlobalState()
                .withSharedState(Jsons.jsonNode(mapOf("wal" to 20)))
                .withStreamStates(
                  listOf(
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName(name1).withNamespace(namespace1))
                      .withStreamState(Jsons.jsonNode(mapOf("id" to 20))),
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName(name2).withNamespace(namespace2))
                      .withStreamState(Jsons.jsonNode(mapOf("id" to 30))),
                  ),
                ),
            ).withSourceStats(AirbyteStateStats().withRecordCount((recordCountStream1 + recordCountStream2).toDouble()))
            .withDestinationStats(AirbyteStateStats().withRecordCount((recordCountStream1 + recordCountStream2).toDouble()))
            .withType(AirbyteStateMessage.AirbyteStateType.GLOBAL),
        ),
      )

    val replicationInputFeatureFlagReader =
      mockk<ReplicationInputFeatureFlagReader> {
        every { read(FailSyncOnInvalidChecksum) } returns true
        every { read(LogStateMsgs) } returns false
      }
    statsTracker.setReplicationFeatureFlagReader(replicationInputFeatureFlagReader)

    assertDoesNotThrow {
      trackRecords(recordCountStream1, name1, namespace1)
      trackRecords(recordCountStream2, name2, namespace2)

      // First assert that the checksums match
      statsTracker.updateSourceStatesStats(stateMessage1.state)
      statsTracker.updateDestinationStateStats(stateMessage1.state)

      trackRecords(recordCountStream1, name1, namespace1)
      trackRecords(recordCountStream2, name2, namespace2)
      statsTracker.updateSourceStatesStats(stateMessage2.state)
      statsTracker.updateDestinationStateStats(stateMessage2.state)
    }
  }

  @Test
  internal fun `test that no exception is raised when the state message checksum comparison passes for global state out of order`() {
    val name1 = "name1"
    val namespace1 = "namespace1"
    val name2 = "name2"
    val namespace2 = "namespace2"
    val recordCountStream1 = 10
    val recordCountStream2 = 15
    val stateMessage1 =
      attachIdToStateMessageFromSource(
        AirbyteMessage().withType(AirbyteMessage.Type.STATE).withState(
          AirbyteStateMessage()
            .withGlobal(
              AirbyteGlobalState()
                .withSharedState(Jsons.jsonNode(mapOf("wal" to 10)))
                .withStreamStates(
                  listOf(
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName(name1).withNamespace(namespace1))
                      .withStreamState(Jsons.jsonNode(mapOf("id" to 10))),
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName(name2).withNamespace(namespace2))
                      .withStreamState(Jsons.jsonNode(mapOf("id" to 15))),
                  ),
                ),
            ).withSourceStats(AirbyteStateStats().withRecordCount((recordCountStream1 + recordCountStream2).toDouble()))
            .withDestinationStats(AirbyteStateStats().withRecordCount((recordCountStream1 + recordCountStream2).toDouble()))
            .withType(AirbyteStateMessage.AirbyteStateType.GLOBAL),
        ),
      )
    val stateMessage2 =
      attachIdToStateMessageFromSource(
        AirbyteMessage().withType(AirbyteMessage.Type.STATE).withState(
          AirbyteStateMessage()
            .withGlobal(
              AirbyteGlobalState()
                .withSharedState(Jsons.jsonNode(mapOf("wal" to 20)))
                .withStreamStates(
                  listOf(
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName(name1).withNamespace(namespace1))
                      .withStreamState(Jsons.jsonNode(mapOf("id" to 20))),
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName(name2).withNamespace(namespace2))
                      .withStreamState(Jsons.jsonNode(mapOf("id" to 30))),
                  ),
                ),
            ).withSourceStats(AirbyteStateStats().withRecordCount((recordCountStream1 + recordCountStream2).toDouble()))
            .withDestinationStats(AirbyteStateStats().withRecordCount((recordCountStream1 + recordCountStream2).toDouble()))
            .withType(AirbyteStateMessage.AirbyteStateType.GLOBAL),
        ),
      )

    val replicationInputFeatureFlagReader =
      mockk<ReplicationInputFeatureFlagReader> {
        every { read(FailSyncOnInvalidChecksum) } returns true
        every { read(LogStateMsgs) } returns false
      }
    statsTracker.setReplicationFeatureFlagReader(replicationInputFeatureFlagReader)

    assertDoesNotThrow {
      trackRecords(recordCountStream1, name1, namespace1)
      trackRecords(recordCountStream2, name2, namespace2)

      // First assert that the checksums match
      statsTracker.updateSourceStatesStats(stateMessage1.state)

      trackRecords(recordCountStream1, name1, namespace1)
      trackRecords(recordCountStream2, name2, namespace2)
      statsTracker.updateSourceStatesStats(stateMessage2.state)

      statsTracker.updateDestinationStateStats(stateMessage1.state)
      statsTracker.updateDestinationStateStats(stateMessage2.state)
    }
  }

  @Test
  internal fun `test that no exception is raised when the state message checksum comparison is disables for global state collision`() {
    val name1 = "name1"
    val namespace1 = "namespace1"
    val name2 = "name2"
    val namespace2 = "namespace2"
    val recordCountStream1 = 10
    val recordCountStream2 = 15
    val stateMessage1 =
      attachIdToStateMessageFromSource(
        AirbyteMessage().withType(AirbyteMessage.Type.STATE).withState(
          AirbyteStateMessage()
            .withGlobal(
              AirbyteGlobalState()
                .withSharedState(Jsons.jsonNode(mapOf("wal" to 10)))
                .withStreamStates(
                  listOf(
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName(name1).withNamespace(namespace1))
                      .withStreamState(Jsons.jsonNode(mapOf("id" to 10))),
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName(name2).withNamespace(namespace2))
                      .withStreamState(Jsons.jsonNode(mapOf("id" to 15))),
                  ),
                ),
            ).withSourceStats(AirbyteStateStats().withRecordCount((recordCountStream1 + recordCountStream2).toDouble()))
            .withDestinationStats(AirbyteStateStats().withRecordCount((recordCountStream1 + recordCountStream2).toDouble()))
            .withType(AirbyteStateMessage.AirbyteStateType.GLOBAL),
        ),
      )

    val replicationInputFeatureFlagReader =
      mockk<ReplicationInputFeatureFlagReader> {
        every { read(FailSyncOnInvalidChecksum) } returns true
        every { read(LogStateMsgs) } returns false
      }
    statsTracker.setReplicationFeatureFlagReader(replicationInputFeatureFlagReader)

    assertDoesNotThrow {
      trackRecords(recordCountStream1, name1, namespace1)
      trackRecords(recordCountStream2, name2, namespace2)

      // First assert that the checksums match
      statsTracker.updateSourceStatesStats(stateMessage1.state)

      trackRecords(recordCountStream1, name1, namespace1)
      trackRecords(recordCountStream2, name2, namespace2)
      statsTracker.updateSourceStatesStats(stateMessage1.state)

      statsTracker.updateDestinationStateStats(stateMessage1.state)
      statsTracker.updateDestinationStateStats(stateMessage1.state)
    }

    assertFalse(statsTracker.isChecksumValidationEnabled())
  }

  @Test
  internal fun `test that an exception is raised when the state message checksum comparison fails for global state`() {
    val name1 = "name1"
    val namespace1 = "namespace1"
    val name2 = "name2"
    val namespace2 = "namespace2"
    val recordCountStream1 = 10
    val recordCountStream2 = 15
    val stateMessage1 =
      attachIdToStateMessageFromSource(
        AirbyteMessage().withType(AirbyteMessage.Type.STATE).withState(
          AirbyteStateMessage()
            .withGlobal(
              AirbyteGlobalState()
                .withSharedState(Jsons.jsonNode(mapOf("wal" to 10)))
                .withStreamStates(
                  listOf(
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName(name1).withNamespace(namespace1))
                      .withStreamState(Jsons.jsonNode(mapOf("id" to 10))),
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName(name2).withNamespace(namespace2))
                      .withStreamState(Jsons.jsonNode(mapOf("id" to 15))),
                  ),
                ),
            ).withSourceStats(AirbyteStateStats().withRecordCount((recordCountStream1 + recordCountStream2).toDouble()))
            .withDestinationStats(AirbyteStateStats().withRecordCount((recordCountStream1 + recordCountStream2).toDouble()))
            .withType(AirbyteStateMessage.AirbyteStateType.GLOBAL),
        ),
      )
    val stateMessage2 =
      attachIdToStateMessageFromSource(
        AirbyteMessage().withType(AirbyteMessage.Type.STATE).withState(
          AirbyteStateMessage()
            .withGlobal(
              AirbyteGlobalState()
                .withSharedState(Jsons.jsonNode(mapOf("wal" to 20)))
                .withStreamStates(
                  listOf(
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName(name1).withNamespace(namespace1))
                      .withStreamState(Jsons.jsonNode(mapOf("id" to 20))),
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName(name2).withNamespace(namespace2))
                      .withStreamState(Jsons.jsonNode(mapOf("id" to 30))),
                  ),
                ),
            ).withSourceStats(AirbyteStateStats().withRecordCount((recordCountStream1 + recordCountStream2).toDouble()))
            .withDestinationStats(AirbyteStateStats().withRecordCount((recordCountStream1 + recordCountStream2).toDouble()))
            .withType(AirbyteStateMessage.AirbyteStateType.GLOBAL),
        ),
      )

    val replicationInputFeatureFlagReader =
      mockk<ReplicationInputFeatureFlagReader> {
        every { read(FailSyncOnInvalidChecksum) } returns true
        every { read(LogStateMsgs) } returns false
      }
    statsTracker.setReplicationFeatureFlagReader(replicationInputFeatureFlagReader)

    trackRecords(recordCountStream1, name1, namespace1)
    trackRecords(recordCountStream2, name2, namespace2)

    // First assert that the checksums match
    statsTracker.updateSourceStatesStats(stateMessage1.state)
    statsTracker.updateDestinationStateStats(stateMessage1.state)

    trackRecords(recordCountStream1, name1, namespace1)
    trackRecords(recordCountStream2, name2, namespace2)
    statsTracker.updateSourceStatesStats(stateMessage2.state)
    assertThrows(InvalidChecksumException::class.java) {
      stateMessage2.state.sourceStats.recordCount = recordCountStream1.toDouble()
      statsTracker.updateDestinationStateStats(stateMessage2.state)
    }
  }

  /**
   * Focus SyncStats comparison on the records related metrics by blanking out the rest.
   */
  private fun assertSyncStatsCoreStatsEquals(
    expected: SyncStats,
    actual: SyncStats,
  ) {
    val strippedExpected = keepCoreStats(expected)
    val strippedActual = keepCoreStats(actual)

    if (strippedExpected != strippedActual) {
      fail<Any>("SyncStats differ, expected ${Jsons.serialize(strippedExpected)}, actual:${Jsons.serialize(strippedActual)}")
    }
  }

  /**
   * List of StreamSyncStats comparison helper.
   */
  private fun assertStreamSyncStatsCoreStatsEquals(
    expected: List<StreamSyncStats>,
    actual: List<StreamSyncStats>,
  ) {
    val filteredExpected = extractCoreStats(expected)
    val filteredActual = extractCoreStats(actual)

    var isDifferent = false

    // checking all the stats from expected are in actual
    for ((key, value) in filteredExpected) {
      val other = filteredActual[key]
      if (other == null) {
        isDifferent = true
        logger.info { "$key is missing from actual" }
      } else if (value != other) {
        isDifferent = true
        logger.info {
          "$key has different stats, expected: ${Jsons.serialize(value)}, got: ${Jsons.serialize(other)}"
        }
      }
    }

    // checking no stats are only in actual
    for ((key) in filteredActual) {
      val other = filteredExpected[key]
      if (other == null) {
        isDifferent = true
        logger.info { "$key is only in actual" }
      }
    }

    assertFalse(isDifferent)
  }

  private fun keepCoreStats(syncStats: SyncStats): SyncStats =
    SyncStats()
      .withBytesCommitted(syncStats.bytesCommitted)
      .withBytesEmitted(syncStats.bytesEmitted)
      .withRecordsCommitted(syncStats.recordsCommitted)
      .withRecordsEmitted(syncStats.recordsEmitted)

  private fun extractCoreStats(streamSyncStatsList: List<StreamSyncStats>): Map<AirbyteStreamNameNamespacePair, SyncStats> {
    val filterStats: MutableMap<AirbyteStreamNameNamespacePair, SyncStats> = HashMap()
    for (streamSyncStats in streamSyncStatsList) {
      val streamNameNamespacesPair =
        AirbyteStreamNameNamespacePair(streamSyncStats.streamName, streamSyncStats.streamNamespace)
      val previous = filterStats.put(streamNameNamespacesPair, keepCoreStats(streamSyncStats.stats))
      if (previous != null) {
        logger.info { "Duplicated stream found: $streamNameNamespacesPair" }
      }
      assertNull(previous)
    }
    return filterStats
  }

  /**
   * Helper function to buildSyncStats for result comparison.
   *
   *
   * For conciseness, we derive the bytes from the number of records.
   */
  private fun buildSyncStats(
    recordsEmitted: Long,
    recordsCommitted: Long,
  ): SyncStats =
    SyncStats()
      .withRecordsEmitted(recordsEmitted)
      .withBytesEmitted(recordsEmitted * MESSAGE_SIZE)
      .withRecordsCommitted(recordsCommitted)
      .withRecordsFilteredOut(0)
      .withBytesFilteredOut(0)
      .withBytesCommitted(recordsCommitted * MESSAGE_SIZE)

  private fun createEstimate(
    streamName: String,
    byteEstimate: Long,
    rowEstimate: Long,
  ): AirbyteEstimateTraceMessage =
    AirbyteMessageUtils
      .createStreamEstimateMessage(
        streamName,
        null,
        byteEstimate,
        rowEstimate,
      ).trace.estimate

  private fun createSyncEstimate(
    byteEstimate: Long,
    rowEstimate: Long,
  ): AirbyteEstimateTraceMessage =
    AirbyteMessageUtils
      .createEstimateMessage(
        AirbyteEstimateTraceMessage.Type.SYNC,
        null,
        null,
        byteEstimate,
        rowEstimate,
      ).trace.estimate

  private fun createRecord(
    streamName: String,
    value: String,
  ): AirbyteRecordMessage = AirbyteMessageUtils.createRecordMessage(streamName, FIELD_NAME, value).record

  private fun createStreamState(
    streamName: String,
    value: Int,
  ): AirbyteStateMessage =
    attachIdToStateMessageFromSource(
      AirbyteMessage()
        .withType(AirbyteMessage.Type.STATE)
        .withState(AirbyteMessageUtils.createStreamStateMessage(streamName, value)),
    ).state

  private fun createGlobalState(
    value: Int,
    vararg streamNames: String,
  ): AirbyteStateMessage {
    val streamStates: MutableList<AirbyteStreamState> = ArrayList()
    for (streamName in streamNames) {
      streamStates.add(AirbyteMessageUtils.createStreamState(streamName).withStreamState(Jsons.jsonNode(value)))
    }
    return attachIdToStateMessageFromSource(
      AirbyteMessage()
        .withType(AirbyteMessage.Type.STATE)
        .withState(
          AirbyteStateMessage()
            .withType(AirbyteStateMessage.AirbyteStateType.GLOBAL)
            .withGlobal(
              AirbyteGlobalState()
                .withStreamStates(streamStates),
            ),
        ),
    ).state
  }

  private fun trackRecords(
    numRecords: Int,
    streamName: String,
    streamNamespace: String,
  ) {
    (1..numRecords).forEach { _ ->
      val record: AirbyteRecordMessage = mockk()
      every { record.data } returns Jsons.jsonNode(mapOf("col1" to "value"))
      every { record.namespace } returns streamNamespace
      every { record.stream } returns streamName
      every { record.fileReference } returns null
      every { record.additionalProperties } returns null
      statsTracker.updateStats(record)
    }
  }
}
