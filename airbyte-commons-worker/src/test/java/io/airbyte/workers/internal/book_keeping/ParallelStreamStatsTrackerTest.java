/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.book_keeping;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.airbyte.commons.json.Jsons;
import io.airbyte.config.StreamSyncStats;
import io.airbyte.config.SyncStats;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.protocol.models.AirbyteEstimateTraceMessage;
import io.airbyte.protocol.models.AirbyteEstimateTraceMessage.Type;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.airbyte.protocol.models.AirbyteStreamNameNamespacePair;
import io.airbyte.workers.test_utils.AirbyteMessageUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Slf4j
class ParallelStreamStatsTrackerTest {

  private static final String FIELD_NAME = "field";
  private static final String STREAM1_NAME = "stream1";
  private static final String STREAM2_NAME = "stream2";

  private static final AirbyteStreamNameNamespacePair STREAM1 = new AirbyteStreamNameNamespacePair(STREAM1_NAME, null);
  private static final AirbyteStreamNameNamespacePair STREAM2 = new AirbyteStreamNameNamespacePair(STREAM2_NAME, null);

  private static final AirbyteRecordMessage S1_MESSAGE1 = createRecord(STREAM1_NAME, "s1m1");
  private static final AirbyteRecordMessage S1_MESSAGE2 = createRecord(STREAM1_NAME, "s1m2");
  private static final AirbyteRecordMessage S1_MESSAGE3 = createRecord(STREAM1_NAME, "s1m3");
  private static final AirbyteRecordMessage S2_MESSAGE1 = createRecord(STREAM2_NAME, "s2m1");
  private static final AirbyteRecordMessage S2_MESSAGE2 = createRecord(STREAM2_NAME, "s2m2");
  private static final AirbyteRecordMessage S2_MESSAGE3 = createRecord(STREAM2_NAME, "s2m3");

  // This is based of the current size of a record from createRecord
  private static final Long MESSAGE_SIZE = 16L;

  private MetricClient metricClient;
  private ParallelStreamStatsTracker statsTracker;

  @BeforeEach
  void beforeEach() {
    metricClient = mock(MetricClient.class);
    statsTracker = new ParallelStreamStatsTracker(metricClient);
  }

  @Test
  void testSerialStreamStatsTracking() {
    statsTracker.updateStats(S1_MESSAGE1);
    statsTracker.updateStats(S1_MESSAGE2);
    final var s1State1 = createState(STREAM1_NAME, 2);
    statsTracker.updateSourceStatesStats(s1State1);
    statsTracker.updateDestinationStateStats(s1State1);
    statsTracker.updateStats(S1_MESSAGE3);

    statsTracker.updateStats(S2_MESSAGE1);
    statsTracker.updateStats(S2_MESSAGE2);
    statsTracker.updateStats(S2_MESSAGE3);

    final SyncStats actualSyncStats = statsTracker.getTotalStats(false);
    final List<StreamSyncStats> actualStreamSyncStats = statsTracker.getAllStreamSyncStats(false);

    final SyncStats expectedSyncStats = buildSyncStats(6L, 2L);
    assertSyncStatsCoreStatsEquals(expectedSyncStats, actualSyncStats);

    final List<StreamSyncStats> expectedStreamSyncStats = List.of(
        new StreamSyncStats()
            .withStreamName(STREAM1_NAME)
            .withStats(buildSyncStats(3L, 2L)),
        new StreamSyncStats()
            .withStreamName(STREAM2_NAME)
            .withStats(buildSyncStats(3L, 0L)));
    assertStreamSyncStatsCoreStatsEquals(expectedStreamSyncStats, actualStreamSyncStats);
  }

  @Test
  void testSerialStreamStatsTrackingOnSingleStream() {
    final var s1State1 = createState(STREAM1_NAME, 1);
    final var s1State2 = createState(STREAM1_NAME, 2);
    final var s1State3 = createState(STREAM1_NAME, 3);

    statsTracker.updateStats(S1_MESSAGE1);
    statsTracker.updateSourceStatesStats(s1State1);
    statsTracker.updateStats(S1_MESSAGE1);
    statsTracker.updateSourceStatesStats(s1State2);
    statsTracker.updateStats(S1_MESSAGE1);
    statsTracker.updateSourceStatesStats(s1State3);

    statsTracker.updateDestinationStateStats(s1State1);
    final SyncStats actualSyncStatsAfter1 = statsTracker.getTotalStats(false);
    assertSyncStatsCoreStatsEquals(buildSyncStats(3L, 1L), actualSyncStatsAfter1);

    statsTracker.updateDestinationStateStats(s1State2);
    final SyncStats actualSyncStatsAfter2 = statsTracker.getTotalStats(false);
    assertSyncStatsCoreStatsEquals(buildSyncStats(3L, 2L), actualSyncStatsAfter2);

    statsTracker.updateDestinationStateStats(s1State3);
    final SyncStats actualSyncStatsAfter3 = statsTracker.getTotalStats(false);
    assertSyncStatsCoreStatsEquals(buildSyncStats(3L, 3L), actualSyncStatsAfter3);
  }

  @Test
  void testSerialStreamStatsTrackingOnSingleStreamWhileSkippingStates() {
    final var s1State1 = createState(STREAM1_NAME, 1);
    final var s1State2 = createState(STREAM1_NAME, 2);
    final var s1State3 = createState(STREAM1_NAME, 3);
    final var s1State4 = createState(STREAM1_NAME, 4);

    statsTracker.updateStats(S1_MESSAGE1);
    statsTracker.updateSourceStatesStats(s1State1);
    statsTracker.updateStats(S1_MESSAGE1);
    statsTracker.updateStats(S1_MESSAGE1);
    statsTracker.updateSourceStatesStats(s1State2);
    statsTracker.updateStats(S1_MESSAGE1);
    statsTracker.updateSourceStatesStats(s1State3);
    statsTracker.updateStats(S1_MESSAGE1);

    statsTracker.updateDestinationStateStats(s1State2);
    final SyncStats actualSyncStatsAfter1 = statsTracker.getTotalStats(false);
    assertSyncStatsCoreStatsEquals(buildSyncStats(5L, 3L), actualSyncStatsAfter1);

    // Adding more messages around the state to also test the emitted tracking logic
    statsTracker.updateStats(S1_MESSAGE1);
    statsTracker.updateSourceStatesStats(s1State4);
    statsTracker.updateStats(S1_MESSAGE1);
    statsTracker.updateStats(S1_MESSAGE1);

    statsTracker.updateDestinationStateStats(s1State4);
    final SyncStats actualSyncStatsAfter2 = statsTracker.getTotalStats(false);
    assertSyncStatsCoreStatsEquals(buildSyncStats(8L, 6L), actualSyncStatsAfter2);
  }

  @Test
  void testSerialStreamStatsTrackingCompletedSync() {
    statsTracker.updateStats(S1_MESSAGE1);
    final var s1State1 = createState(STREAM1_NAME, 1);
    statsTracker.updateSourceStatesStats(s1State1);
    statsTracker.updateDestinationStateStats(s1State1);

    statsTracker.updateStats(S2_MESSAGE1);
    statsTracker.updateStats(S2_MESSAGE2);
    statsTracker.updateStats(S2_MESSAGE3);
    final var s2State1 = createState(STREAM2_NAME, 3);
    statsTracker.updateSourceStatesStats(s2State1);
    statsTracker.updateDestinationStateStats(s2State1);

    // Worth noting, in the current implementation, if replication has completed, we assume all records
    // to be committed, even though there is no state messages after.
    statsTracker.updateStats(S1_MESSAGE2);

    final SyncStats actualSyncStats = statsTracker.getTotalStats(true);
    final List<StreamSyncStats> actualStreamSyncStats = statsTracker.getAllStreamSyncStats(true);

    final SyncStats expectedSyncStats = buildSyncStats(5L, 5L);
    assertSyncStatsCoreStatsEquals(expectedSyncStats, actualSyncStats);

    final List<StreamSyncStats> expectedStreamSyncStats = List.of(
        new StreamSyncStats()
            .withStreamName(STREAM1_NAME)
            .withStats(buildSyncStats(2L, 2L)),
        new StreamSyncStats()
            .withStreamName(STREAM2_NAME)
            .withStats(buildSyncStats(3L, 3L)));
    assertStreamSyncStatsCoreStatsEquals(expectedStreamSyncStats, actualStreamSyncStats);
  }

  @Test
  void testParallelStreamStatsTracking() {
    statsTracker.updateStats(S1_MESSAGE1);
    statsTracker.updateStats(S2_MESSAGE1);
    statsTracker.updateStats(S1_MESSAGE2);
    final var s1State1 = createState(STREAM1_NAME, 2);
    statsTracker.updateSourceStatesStats(s1State1);
    statsTracker.updateStats(S2_MESSAGE2);
    statsTracker.updateStats(S1_MESSAGE3);
    final var s1State2 = createState(STREAM1_NAME, 3);
    statsTracker.updateSourceStatesStats(s1State2);
    statsTracker.updateDestinationStateStats(s1State1);

    // At this point, only s1state1 has been committed.
    final SyncStats midSyncCheckpoint1Stats = statsTracker.getTotalStats(false);
    final SyncStats expectedMidSyncCheckpoint1Stats = buildSyncStats(5L, 2L);
    assertSyncStatsCoreStatsEquals(expectedMidSyncCheckpoint1Stats, midSyncCheckpoint1Stats);

    // Sending more state for stream 2
    final var s2State1 = createState(STREAM2_NAME, 2);
    statsTracker.updateSourceStatesStats(s2State1);
    statsTracker.updateDestinationStateStats(s2State1);

    // We should now have data for stream two as well
    final SyncStats midSyncCheckpoint2Stats = statsTracker.getTotalStats(false);
    final SyncStats expectedMidSyncCheckpoint2Stats = buildSyncStats(5L, 4L);
    assertSyncStatsCoreStatsEquals(expectedMidSyncCheckpoint2Stats, midSyncCheckpoint2Stats);

    // Closing up states
    statsTracker.updateDestinationStateStats(s1State2);
    final SyncStats midSyncCheckpoint3Stats = statsTracker.getTotalStats(false);
    final SyncStats expectedMidSyncCheckpoint3Stats = buildSyncStats(5L, 5L);
    assertSyncStatsCoreStatsEquals(expectedMidSyncCheckpoint3Stats, midSyncCheckpoint3Stats);
  }

  @Test
  void testDuplicatedSourceStates() {
    final var s1State1 = createState(STREAM1_NAME, 1);
    final var s1State2 = createState(STREAM1_NAME, 2);
    final var s2State1 = createState(STREAM2_NAME, 1);
    statsTracker.updateStats(S1_MESSAGE1);
    statsTracker.updateSourceStatesStats(s1State1);
    statsTracker.updateDestinationStateStats(s1State1);
    statsTracker.updateStats(S1_MESSAGE2);
    statsTracker.updateSourceStatesStats(s1State2);
    statsTracker.updateSourceStatesStats(s1State2); // We will drop mid-sync committed stats for the stream because of this
    statsTracker.updateDestinationStateStats(s1State2);
    statsTracker.updateStats(S2_MESSAGE1);
    statsTracker.updateSourceStatesStats(s2State1);
    statsTracker.updateDestinationStateStats(s2State1);

    final SyncStats actualMidSyncSyncStats = statsTracker.getTotalStats(false);
    assertSyncStatsCoreStatsEquals(buildSyncStats(3L, 2L), actualMidSyncSyncStats);

    // hasReplicationCompleted: true, means the sync was successful, we still expect committed to equal
    // emitted in this case.
    final SyncStats actualSyncStats = statsTracker.getTotalStats(true);
    assertSyncStatsCoreStatsEquals(buildSyncStats(3L, 3L), actualSyncStats);

    verify(metricClient).count(OssMetricsRegistry.STATE_ERROR_COLLISION_FROM_SOURCE, 1);

    // The following metrics are expected to be discarded
    assertEquals(Optional.empty(), statsTracker.getMaxSecondsBetweenStateMessageEmittedAndCommitted());
    assertEquals(Optional.empty(), statsTracker.getMeanSecondsBetweenStateMessageEmittedAndCommitted());
  }

  @Test
  void testUnexpectedStateFromDestination() {
    final var s1State1 = createState(STREAM1_NAME, 1);
    final var s1State2 = createState(STREAM1_NAME, 2);

    statsTracker.updateStats(S1_MESSAGE1);
    statsTracker.updateSourceStatesStats(s1State1);
    statsTracker.updateDestinationStateStats(createState(STREAM1_NAME, 5)); // This is unexpected since it never came from the source.
    statsTracker.updateDestinationStateStats(s1State1);
    statsTracker.updateStats(S1_MESSAGE2);
    statsTracker.updateSourceStatesStats(s1State2);
    statsTracker.updateDestinationStateStats(s1State2);

    final SyncStats actualMidSyncSyncStats = statsTracker.getTotalStats(false);
    assertSyncStatsCoreStatsEquals(buildSyncStats(2L, 2L), actualMidSyncSyncStats);

    verify(metricClient).count(OssMetricsRegistry.STATE_ERROR_UNKNOWN_FROM_DESTINATION, 1);
  }

  @Test
  void testReceivingTheSameStateFromDestinationDoesntFlushUnexpectedStates() {
    final var s1State1 = createState(STREAM1_NAME, 1);
    final var s1State2 = createState(STREAM1_NAME, 2);
    final var s1State3 = createState(STREAM1_NAME, 3);

    statsTracker.updateStats(S1_MESSAGE1);
    statsTracker.updateSourceStatesStats(s1State1);
    statsTracker.updateStats(S1_MESSAGE1);
    statsTracker.updateSourceStatesStats(s1State2);
    statsTracker.updateStats(S1_MESSAGE1);
    statsTracker.updateSourceStatesStats(s1State3);

    // Sending state 2 should clear state1 and state2
    statsTracker.updateDestinationStateStats(s1State2);
    final SyncStats statsAfterState2 = statsTracker.getTotalStats(false);
    assertSyncStatsCoreStatsEquals(buildSyncStats(3L, 2L), statsAfterState2);

    // Sending state 1 out of order
    statsTracker.updateDestinationStateStats(s1State1);
    final SyncStats statsAfterState1OutOfOrder = statsTracker.getTotalStats(false);
    // Stats count should remain stable because state1 has already been handled
    assertSyncStatsCoreStatsEquals(buildSyncStats(3L, 2L), statsAfterState1OutOfOrder);
    verify(metricClient, times(1)).count(OssMetricsRegistry.STATE_ERROR_UNKNOWN_FROM_DESTINATION, 1);

    // Sending state 2 again
    statsTracker.updateDestinationStateStats(s1State2);
    final SyncStats statsAfterState2Again = statsTracker.getTotalStats(false);
    // Stats count should remain stable because state1 has already been handled
    assertSyncStatsCoreStatsEquals(buildSyncStats(3L, 2L), statsAfterState2Again);
    verify(metricClient, times(2)).count(OssMetricsRegistry.STATE_ERROR_UNKNOWN_FROM_DESTINATION, 1);

    // Sending state 3
    reset(metricClient);
    statsTracker.updateDestinationStateStats(s1State3);
    final SyncStats statsAfterState3 = statsTracker.getTotalStats(false);
    // Stats count should remain stable because state1 has already been handled
    assertSyncStatsCoreStatsEquals(buildSyncStats(3L, 3L), statsAfterState3);
    verify(metricClient, never()).count(OssMetricsRegistry.STATE_ERROR_UNKNOWN_FROM_DESTINATION, 1);

    // Sending state 3 again
    statsTracker.updateDestinationStateStats(s1State3);
    final SyncStats statsAfterState3Again = statsTracker.getTotalStats(false);
    // Stats count should remain stable because state1 has already been handled
    assertSyncStatsCoreStatsEquals(buildSyncStats(3L, 3L), statsAfterState3Again);
    verify(metricClient, times(1)).count(OssMetricsRegistry.STATE_ERROR_UNKNOWN_FROM_DESTINATION, 1);
  }

  @Test
  void testAccessors() {
    statsTracker.updateStats(S2_MESSAGE1);
    statsTracker.updateStats(S1_MESSAGE1);
    statsTracker.updateStats(S2_MESSAGE1);
    final var s1State1 = createState(STREAM1_NAME, 1);
    final var s2State1 = createState(STREAM2_NAME, 2);
    statsTracker.updateSourceStatesStats(s1State1);
    statsTracker.updateSourceStatesStats(s2State1);
    statsTracker.updateDestinationStateStats(s2State1);

    assertEquals(Optional.of(Map.of(STREAM1, 0L, STREAM2, 2L)), statsTracker.getStreamToCommittedRecords());
    assertEquals(Optional.of(Map.of(STREAM1, 0L, STREAM2, 2L * MESSAGE_SIZE)), statsTracker.getStreamToCommittedBytes());

    assertEquals(Map.of(STREAM1, 1L, STREAM2, 2L), statsTracker.getStreamToEmittedRecords());
    assertEquals(Map.of(STREAM1, 1L * MESSAGE_SIZE, STREAM2, 2L * MESSAGE_SIZE), statsTracker.getStreamToEmittedBytes());

    assertEquals(3L, statsTracker.getTotalRecordsEmitted());
    assertEquals(3L * MESSAGE_SIZE, statsTracker.getTotalBytesEmitted());
    assertEquals(2L, statsTracker.getTotalRecordsCommitted().get());
    assertEquals(2L * MESSAGE_SIZE, statsTracker.getTotalBytesCommitted().get());
  }

  @Test
  void testStreamEstimates() {
    final var estimateStream1Message1 = createEstimate(STREAM1_NAME, 1L, 1L);
    final var estimateStream1Message2 = createEstimate(STREAM1_NAME, 10L, 2L);
    final var estimateStream2 = createEstimate(STREAM2_NAME, 100L, 21L);

    // Note that estimates are global, we override the count for each message rather than sum
    statsTracker.updateEstimates(estimateStream1Message1);
    statsTracker.updateEstimates(estimateStream1Message2);
    statsTracker.updateEstimates(estimateStream2);

    final SyncStats actualSyncStats = statsTracker.getTotalStats(false);
    assertEquals(buildSyncStats(0L, 0L).withEstimatedBytes(110L).withEstimatedRecords(23L), actualSyncStats);

    final List<StreamSyncStats> actualStreamSyncStats = statsTracker.getAllStreamSyncStats(false);
    final List<StreamSyncStats> expectedStreamSyncStats = List.of(
        new StreamSyncStats()
            .withStreamName(STREAM1_NAME)
            .withStats(buildSyncStats(0L, 0L).withEstimatedBytes(10L).withEstimatedRecords(2L)),
        new StreamSyncStats()
            .withStreamName(STREAM2_NAME)
            .withStats(buildSyncStats(0L, 0L).withEstimatedBytes(100L).withEstimatedRecords(21L)));
    assertStreamSyncStatsCoreStatsEquals(expectedStreamSyncStats, actualStreamSyncStats);

    assertEquals(23L, statsTracker.getTotalRecordsEstimated());
    assertEquals(110L, statsTracker.getTotalBytesEstimated());
    assertEquals(Map.of(STREAM1, 2L, STREAM2, 21L), statsTracker.getStreamToEstimatedRecords());
    assertEquals(Map.of(STREAM1, 10L, STREAM2, 100L), statsTracker.getStreamToEstimatedBytes());
  }

  @Test
  void testSyncEstimates() {
    final var syncEstimate1 = createSyncEstimate(2L, 1L);
    final var syncEstimate2 = createSyncEstimate(15L, 5L);
    statsTracker.updateEstimates(syncEstimate1);
    statsTracker.updateEstimates(syncEstimate2);

    final SyncStats actualSyncStats = statsTracker.getTotalStats(false);
    assertEquals(new SyncStats().withEstimatedBytes(15L).withEstimatedRecords(5L), actualSyncStats);
    assertEquals(List.of(), statsTracker.getAllStreamSyncStats(false));

    assertEquals(5L, statsTracker.getTotalRecordsEstimated());
    assertEquals(15L, statsTracker.getTotalBytesEstimated());
    assertEquals(Map.of(), statsTracker.getStreamToEstimatedRecords());
    assertEquals(Map.of(), statsTracker.getStreamToEstimatedBytes());
  }

  @Test
  void testEstimateTypeConflicts() {
    statsTracker.updateEstimates(createEstimate(STREAM2_NAME, 4L, 2L));
    statsTracker.updateEstimates(createSyncEstimate(3L, 1L));

    final SyncStats actualSyncStats = statsTracker.getTotalStats(false);
    assertEquals(buildSyncStats(0L, 0L), actualSyncStats);
    assertEquals(0L, statsTracker.getTotalRecordsEstimated());
    assertEquals(0L, statsTracker.getTotalBytesEstimated());
    assertEquals(Map.of(), statsTracker.getStreamToEstimatedBytes());
    assertEquals(Map.of(), statsTracker.getStreamToEstimatedRecords());
  }

  @Test
  void testCheckpointingMetrics() throws InterruptedException {
    final var s1State1 = createState(STREAM1_NAME, 1);
    final var s1State2 = createState(STREAM1_NAME, 2);
    final var s2State1 = createState(STREAM2_NAME, 1);
    final var s2State2 = createState(STREAM2_NAME, 3);

    statsTracker.updateSourceStatesStats(s1State1);
    statsTracker.updateSourceStatesStats(s2State1);
    Thread.sleep(1000);
    statsTracker.updateSourceStatesStats(s1State2);
    Thread.sleep(1000);
    statsTracker.updateSourceStatesStats(s2State2);
    statsTracker.updateDestinationStateStats(s1State1);
    statsTracker.updateDestinationStateStats(s2State1);

    assertEquals(4, statsTracker.getTotalSourceStateMessagesEmitted());
    assertEquals(2, statsTracker.getTotalDestinationStateMessagesEmitted());

    // We only check for a non-zero value for sanity check to avoid jitter from time.
    assertTrue(statsTracker.getMaxSecondsToReceiveSourceStateMessage() > 0);
    assertTrue(statsTracker.getMeanSecondsToReceiveSourceStateMessage() > 0);
    assertTrue(statsTracker.getMaxSecondsBetweenStateMessageEmittedAndCommitted().get() > 0);
    assertTrue(statsTracker.getMeanSecondsBetweenStateMessageEmittedAndCommitted().get() > 0);
  }

  /**
   * Focus SyncStats comparison on the records related metrics by blanking out the rest.
   */
  private static void assertSyncStatsCoreStatsEquals(final SyncStats expected, final SyncStats actual) {
    final SyncStats strippedExpected = keepCoreStats(expected);
    final SyncStats strippedActual = keepCoreStats(actual);

    if (!strippedExpected.equals(strippedActual)) {
      fail(String.format("SyncStats differ, expected %s, actual:%s", Jsons.serialize(strippedExpected), Jsons.serialize(strippedActual)));
    }
  }

  /**
   * List of StreamSyncStats comparison helper.
   */
  private static void assertStreamSyncStatsCoreStatsEquals(final List<StreamSyncStats> expected, final List<StreamSyncStats> actual) {
    final Map<AirbyteStreamNameNamespacePair, SyncStats> filteredExpected = extractCoreStats(expected);
    final Map<AirbyteStreamNameNamespacePair, SyncStats> filteredActual = extractCoreStats(actual);

    boolean isDifferent = false;

    // checking all the stats from expected are in actual
    for (final var entry : filteredExpected.entrySet()) {
      final var other = filteredActual.get(entry.getKey());
      if (other == null) {
        isDifferent = true;
        log.info("{} is missing from actual", entry.getKey());
      } else if (!entry.getValue().equals(other)) {
        isDifferent = true;
        log.info("{} has different stats, expected:{}, got:{}", entry.getKey(), Jsons.serialize(entry.getValue()), Jsons.serialize(other));
      }
    }

    // checking no stats are only in actual
    for (final var entry : filteredActual.entrySet()) {
      final var other = filteredExpected.get(entry.getKey());
      if (other == null) {
        isDifferent = true;
        log.info("{} is only in actual", entry.getKey());
      }
    }

    assertFalse(isDifferent);
  }

  private static SyncStats keepCoreStats(final SyncStats syncStats) {
    return new SyncStats()
        .withBytesCommitted(syncStats.getBytesCommitted())
        .withBytesEmitted(syncStats.getBytesEmitted())
        .withRecordsCommitted(syncStats.getRecordsCommitted())
        .withRecordsEmitted(syncStats.getRecordsEmitted());
  }

  private static Map<AirbyteStreamNameNamespacePair, SyncStats> extractCoreStats(final List<StreamSyncStats> streamSyncStatsList) {
    final Map<AirbyteStreamNameNamespacePair, SyncStats> filterStats = new HashMap<>();
    for (final var streamSyncStats : streamSyncStatsList) {
      final var streamNameNamespacesPair = new AirbyteStreamNameNamespacePair(streamSyncStats.getStreamName(), streamSyncStats.getStreamNamespace());
      final SyncStats previous = filterStats.put(streamNameNamespacesPair, keepCoreStats(streamSyncStats.getStats()));
      if (previous != null) {
        log.info("Duplicated stream found: {}", streamNameNamespacesPair);
      }
      assertNull(previous);
    }
    return filterStats;
  }

  /**
   * Helper function to buildSyncStats for result comparison.
   * <p>
   * For conciseness, we derive the bytes from the number of records.
   */
  private static SyncStats buildSyncStats(final Long recordsEmitted, final Long recordsCommitted) {
    return new SyncStats()
        .withRecordsEmitted(recordsEmitted)
        .withBytesEmitted(recordsEmitted * MESSAGE_SIZE)
        .withRecordsCommitted(recordsCommitted)
        .withBytesCommitted(recordsCommitted * MESSAGE_SIZE);
  }

  private static AirbyteEstimateTraceMessage createEstimate(final String streamName, final Long byteEstimate, final Long rowEstimate) {
    return AirbyteMessageUtils.createStreamEstimateMessage(streamName, null, byteEstimate, rowEstimate).getTrace().getEstimate();
  }

  private static AirbyteEstimateTraceMessage createSyncEstimate(final Long byteEstimate, final Long rowEstimate) {
    return AirbyteMessageUtils.createEstimateMessage(Type.SYNC, null, null, byteEstimate, rowEstimate).getTrace().getEstimate();
  }

  private static AirbyteRecordMessage createRecord(final String streamName, final String value) {
    return AirbyteMessageUtils.createRecordMessage(streamName, FIELD_NAME, value).getRecord();
  }

  private static AirbyteStateMessage createState(final String streamName, final int value) {
    return AirbyteMessageUtils.createStreamStateMessage(streamName, value);
  }

}
