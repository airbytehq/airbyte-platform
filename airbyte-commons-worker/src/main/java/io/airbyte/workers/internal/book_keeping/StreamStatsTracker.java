/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.book_keeping;

import com.google.common.base.Charsets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.AtomicDouble;
import io.airbyte.commons.json.Jsons;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.protocol.models.AirbyteEstimateTraceMessage;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.airbyte.protocol.models.AirbyteStateMessage.AirbyteStateType;
import io.airbyte.protocol.models.AirbyteStreamNameNamespacePair;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAccumulator;
import lombok.extern.slf4j.Slf4j;

/**
 * Track Stats for a specific stream.
 * <p>
 * Most stats are tracked on the StreamStatsCounters instance as the messages or states ar emitted.
 * <p>
 * In order to track the number of committed records, we count emitted records in between states. To
 * do so, we track messages in a EmittedStatsCounters, when we see a state message from a source, we
 * add the current (State, EmittedStatsCounters) to a list. When we see a state message back from
 * the destination, we pop the corresponding EmittedStatsCounters and update the global committed
 * records count.
 */
@Slf4j
public class StreamStatsTracker {

  /**
   * Record for tracking stats of a given stream, this is also how stats are returned to the outside.
   * <p>
   * All the counters are implemented as AtomicLong to avoid race conditions on updates.
   */
  public record StreamStatsCounters(AtomicLong emittedRecordsCount,
                                    AtomicLong emittedBytesCount,
                                    AtomicLong committedRecordsCount,
                                    AtomicLong committedBytesCount,
                                    AtomicLong estimatedRecordsCount,
                                    AtomicLong estimatedBytesCount,
                                    AtomicLong sourceStateCount,
                                    AtomicLong destinationStateCount,
                                    LongAccumulator maxSecondsToReceiveState,
                                    AtomicDouble meanSecondsToReceiveState,
                                    LongAccumulator maxSecondsBetweenStateEmittedAndCommitted,
                                    AtomicDouble meanSecondsBetweenStateEmittedAndCommitted,
                                    AtomicBoolean unreliableStateOperations) {

    public StreamStatsCounters() {
      this(new AtomicLong(), new AtomicLong(), new AtomicLong(), new AtomicLong(), new AtomicLong(), new AtomicLong(), new AtomicLong(),
          new AtomicLong(), new LongAccumulator(Long::max, 0), new AtomicDouble(), new LongAccumulator(Long::max, 0), new AtomicDouble(),
          new AtomicBoolean(false));
    }

  }

  /**
   * Record for tracking Emitted stats.
   * <p>
   * EmittedStatsCounters are the stats tied to a State that hasn't been acked back from the
   * destination yet. Those stats are "emitted". They will eventually add up to the committed stats
   * once the state is acked by the destination.
   */
  private record EmittedStatsCounters(AtomicLong emittedRecordsCount, AtomicLong emittedBytesCount) {

    public EmittedStatsCounters() {
      this(new AtomicLong(0L), new AtomicLong(0L));
    }

  }

  /**
   * Record for tracking stats that have been staged and are waiting for the state ack from the
   * destination to become committed.
   * <p>
   * This record should track the number of records/bytes emitted that are associated to a given
   * state.
   */
  private record StagedStats(int stateHash,
                             AirbyteStateMessage stateMessage,
                             EmittedStatsCounters emittedStatsCounters,
                             LocalDateTime receivedTime) {}

  private final MetricClient metricClient;
  private final AirbyteStreamNameNamespacePair nameNamespacePair;
  private final HashFunction hashFunction;

  // Tracks the stateHashes that are currently in the rolledStatsList to avoid having to do a full
  // scan to detect issues.
  private final Set<Integer> stateHashes;

  // List <State, Stats> that are waiting for a state message from the destination to be committed.
  private final Queue<StagedStats> stagedStatsList;

  // Global stats counters. Track all the metrics that doesn't depend on a state being committed.
  private final StreamStatsCounters streamStats;

  // Track the metrics that will be updated once a state is being acked by the destination.
  // This is the counters for the in-flight records since the previous state that was read from a
  // source.
  private EmittedStatsCounters emittedStats;

  private LocalDateTime previousStateMessageReceivedAt;

  public StreamStatsTracker(final AirbyteStreamNameNamespacePair nameNamespacePair, final MetricClient metricClient) {
    this.metricClient = metricClient;
    this.nameNamespacePair = nameNamespacePair;
    this.hashFunction = Hashing.murmur3_32_fixed();
    this.stateHashes = ConcurrentHashMap.newKeySet();
    this.stagedStatsList = new ConcurrentLinkedQueue<>();
    this.streamStats = new StreamStatsCounters();

    this.emittedStats = new EmittedStatsCounters();
  }

  /**
   * Bookkeeping for when a record message is read.
   * <p>
   * We update emitted records count on both emittedStats and streamStats. emittedStats is the tracker
   * for what is going to become committed once the state is acked. We update the global count to
   * avoid having to traverse the map to get the global count.
   */
  public void trackRecord(final AirbyteRecordMessage recordMessage) {
    final int estimatedBytesSize = Jsons.getEstimatedByteSize(recordMessage.getData());

    // Update the current emitted stats
    // We do a local copy of the reference to emittedStats to ensure all the stats are
    // updated on the same instance in case the stats were to be staged. This would happen
    // if trackStateFromSource were called in parallel. The alternative would be to use a
    // ReadWriteLock where trackRecord would acquire the read and trackStateFromSource would
    // acquire the write.
    final EmittedStatsCounters emittedStatsToUpdate = emittedStats;
    emittedStatsToUpdate.emittedRecordsCount.incrementAndGet();
    emittedStatsToUpdate.emittedBytesCount.addAndGet(estimatedBytesSize);

    // Update the global stream stats
    streamStats.emittedRecordsCount.incrementAndGet();
    streamStats.emittedBytesCount.addAndGet(estimatedBytesSize);
  }

  /**
   * Bookkeeping for when a state is read from the source.
   * <p>
   * Reading a state from the source means that we need to stage our current counters. All the
   * in-flight records since the last state are associated to this state and will be kept aside until
   * we see that same state back from the destination. We should also recreate a new set of counters
   * to keep on tracking incoming messages.
   */
  public void trackStateFromSource(final AirbyteStateMessage stateMessage) {
    final LocalDateTime currentTime = LocalDateTime.now();
    streamStats.sourceStateCount.incrementAndGet();

    if (streamStats.unreliableStateOperations.get()) {
      // State collision previously detected, we skip all operations that involve state tracking.
      return;
    }

    final int stateHash = getStateHashCode(stateMessage);
    if (!stateHashes.add(stateHash)) {
      // State collision detected, it means that state tracking is compromised for this stream.
      // Rather than reporting incorrect data, we skip all operations that involve state tracking.
      streamStats.unreliableStateOperations.set(true);

      // We can clear the stagedStatsList since we won't be processing it anymore.
      stagedStatsList.clear();

      log.info("State collision detected for stream name({}), stream namespace({})", nameNamespacePair.getName(), nameNamespacePair.getNamespace());
      metricClient.count(OssMetricsRegistry.STATE_ERROR_COLLISION_FROM_SOURCE, 1);
      return;
    }

    // Rollover stat bucket
    final EmittedStatsCounters previousEmittedStats = emittedStats;
    emittedStats = new EmittedStatsCounters();

    stagedStatsList.add(new StagedStats(stateHash, stateMessage, previousEmittedStats, currentTime));

    // Updating state checkpointing metrics
    // previsousStateMessageReceivedAt is null when it's the first state message of a stream.
    if (previousStateMessageReceivedAt != null) {
      final long timeSinceLastState = previousStateMessageReceivedAt.until(currentTime, ChronoUnit.SECONDS);
      streamStats.maxSecondsToReceiveState.accumulate(timeSinceLastState);
      // We are measuring intervals in-between states, so there's going to be one less interval than the
      // number of states.
      streamStats.meanSecondsToReceiveState.set(updateMean(streamStats.meanSecondsToReceiveState.get(), streamStats.sourceStateCount.get() - 1,
          (double) timeSinceLastState));
    }
    previousStateMessageReceivedAt = currentTime;
  }

  /**
   * Bookkeeping for when we see a state from a destination.
   * <p>
   * The current contract with a destination is: a state message means that all records preceding this
   * state message have been persisted, destination may skip state message.
   * <p>
   * We will un-queue all the StagedStats and add them to the global counters as committed until the
   * said acked state.
   */
  public void trackStateFromDestination(final AirbyteStateMessage stateMessage) {
    final LocalDateTime currentTime = LocalDateTime.now();
    streamStats.destinationStateCount.incrementAndGet();

    if (streamStats.unreliableStateOperations.get()) {
      // State collision previously detected, we skip all operations that involve state tracking.
      return;
    }

    final int stateHash = getStateHashCode(stateMessage);
    if (!stateHashes.contains(stateHash) || stagedStatsList.isEmpty()) {
      // Unexpected state from Destination
      log.info("Unexpected state from destination for stream name({}), stream namespace({})", nameNamespacePair.getName(),
          nameNamespacePair.getNamespace());
      metricClient.count(OssMetricsRegistry.STATE_ERROR_UNKNOWN_FROM_DESTINATION, 1);

      // We exit to avoid further tracking or stats un-staging since the state is unknown.
      // However, we don't abort state tracking as we may still recover.
      return;
    }

    StagedStats stagedStats = null;
    // Un-stage stats until the stateMessage.
    while (!stagedStatsList.isEmpty()) {
      stagedStats = stagedStatsList.poll();
      // Cleaning up stateHashes as we go to avoid un-staging on duplicate or our of order state messages
      stateHashes.remove(stagedStats.stateHash);

      // Increment committed stats as we are un-staging stats
      streamStats.committedBytesCount.addAndGet(stagedStats.emittedStatsCounters.emittedBytesCount.get());
      streamStats.committedRecordsCount.addAndGet(stagedStats.emittedStatsCounters.emittedRecordsCount.get());

      if (stagedStats.stateHash == stateHash) {
        break;
      }
    }

    // Updating state checkpointing metrics
    final long durationBetweenStateEmittedAndCommitted = stagedStats.receivedTime.until(currentTime, ChronoUnit.SECONDS);
    streamStats.maxSecondsBetweenStateEmittedAndCommitted.accumulate(durationBetweenStateEmittedAndCommitted);
    streamStats.meanSecondsBetweenStateEmittedAndCommitted.set(
        updateMean(streamStats.meanSecondsBetweenStateEmittedAndCommitted.get(), streamStats.destinationStateCount.get() - 1,
            (double) durationBetweenStateEmittedAndCommitted));
  }

  /**
   * Bookkeeping for when we see an estimate message.
   */
  public void trackEstimates(final AirbyteEstimateTraceMessage estimateMessage) {
    streamStats.estimatedRecordsCount.set(estimateMessage.getRowEstimate());
    streamStats.estimatedBytesCount.set(estimateMessage.getByteEstimate());
  }

  public AirbyteStreamNameNamespacePair getNameNamespacePair() {
    return nameNamespacePair;
  }

  public StreamStatsCounters getStreamStats() {
    return streamStats;
  }

  /**
   * Helper to hash a state to avoid a full state string comparison when looking up states.
   */
  private int getStateHashCode(final AirbyteStateMessage stateMessage) {
    if (AirbyteStateType.GLOBAL == stateMessage.getType()) {
      return hashFunction.hashBytes(Jsons.serialize(stateMessage.getGlobal()).getBytes(Charsets.UTF_8)).hashCode();
    } else if (AirbyteStateType.STREAM == stateMessage.getType()) {
      return hashFunction.hashBytes(Jsons.serialize(stateMessage.getStream().getStreamState()).getBytes(Charsets.UTF_8)).hashCode();
    } else {
      // state type is LEGACY
      return hashFunction.hashBytes(Jsons.serialize(stateMessage.getData()).getBytes(Charsets.UTF_8)).hashCode();
    }
  }

  private Long updateMean(final Double previousMean, final Long previousCount, final Double newDataPoint) {
    final double totalCount = previousCount + 1;
    final double updatedMean = ((previousMean * previousCount) + newDataPoint) / totalCount;
    return (long) updatedMean;
  }

}
