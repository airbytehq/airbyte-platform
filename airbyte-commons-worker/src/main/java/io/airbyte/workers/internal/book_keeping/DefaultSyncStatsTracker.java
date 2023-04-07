/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.book_keeping;

import static io.airbyte.protocol.models.AirbyteEstimateTraceMessage.Type.STREAM;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.airbyte.commons.json.Jsons;
import io.airbyte.protocol.models.AirbyteEstimateTraceMessage;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.airbyte.protocol.models.AirbyteStateMessage.AirbyteStateType;
import io.airbyte.protocol.models.AirbyteStreamNameNamespacePair;
import io.airbyte.workers.internal.book_keeping.StateMetricsTracker.StateMetricsTrackerNoStateMatchException;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Keep track of the metrics we persist for a sync.
 */
@Slf4j
public class DefaultSyncStatsTracker implements SyncStatsTracker {

  private static final long STATE_DELTA_TRACKER_MEMORY_LIMIT_BYTES = 10L * 1024L * 1024L; // 10 MiB, ~5% of default cloud worker memory
  private static final long STATE_METRICS_TRACKER_MESSAGE_LIMIT = 873813L; // 12 bytes per message tracked, maximum of 10MiB of memory

  private final Map<AirbyteStreamNameNamespacePair, StreamStats> nameNamespacePairToStreamStats;
  // These variables support SYNC level estimates and are meant for sources where stream level
  // estimates are not possible e.g. CDC sources.
  private Long totalRecordsEstimatedSync;
  private Long totalBytesEstimatedSync;
  private Optional<Boolean> hasStreamEstimates;

  private final Map<Short, StatsCounters> streamToRunningCount;
  private final HashFunction hashFunction;
  private final StateDeltaTracker stateDeltaTracker;
  private final StateMetricsTracker stateMetricsTracker;
  /**
   * If the StateDeltaTracker throws an exception, this flag is set to true and committed counts are
   * not returned.
   */
  private boolean unreliableCommittedCounts;
  /**
   * If the StateMetricsTracker throws an exception, this flag is set to true and the metrics around
   * max and mean time between state message emitted and committed are unreliable.
   */
  private boolean unreliableStateTimingMetrics;

  // TODO remove? looks like some optimization, not sure it's worth it
  private final BiMap<AirbyteStreamNameNamespacePair, Short> nameNamespacePairToIndex;
  private short nextStreamIndex;

  public DefaultSyncStatsTracker() {
    this(new StateDeltaTracker(STATE_DELTA_TRACKER_MEMORY_LIMIT_BYTES), new StateMetricsTracker(STATE_METRICS_TRACKER_MESSAGE_LIMIT));
  }

  public DefaultSyncStatsTracker(final StateDeltaTracker stateDeltaTracker, final StateMetricsTracker stateMetricsTracker) {
    this.nameNamespacePairToStreamStats = new HashMap<>();
    this.hasStreamEstimates = Optional.empty();

    this.streamToRunningCount = new HashMap<>();
    this.hashFunction = Hashing.murmur3_32_fixed();
    this.stateDeltaTracker = stateDeltaTracker;
    this.stateMetricsTracker = stateMetricsTracker;
    this.unreliableCommittedCounts = false;
    this.unreliableStateTimingMetrics = false;

    this.nameNamespacePairToIndex = HashBiMap.create();
    this.nextStreamIndex = 0;
  }

  /**
   * Update the stats count with data from recordMessage.
   */
  @Override
  public void updateStats(final AirbyteRecordMessage recordMessage) {
    if (stateMetricsTracker.getFirstRecordReceivedAt() == null) {
      stateMetricsTracker.setFirstRecordReceivedAt(LocalDateTime.now());
    }

    final var nameNamespace = AirbyteStreamNameNamespacePair.fromRecordMessage(recordMessage);
    final short streamIndex = getStreamIndex(nameNamespace);

    StatsCounters currentRunningCount = streamToRunningCount.get(streamIndex);
    if (currentRunningCount == null) {
      currentRunningCount = new StatsCounters();
      streamToRunningCount.put(streamIndex, currentRunningCount);
    }
    currentRunningCount.recordCount++;

    final var currStats = nameNamespacePairToStreamStats.getOrDefault(nameNamespace, new StreamStats());
    currStats.emittedRecords++;

    final int estimatedNumBytes = Jsons.getEstimatedByteSize(recordMessage.getData());
    currStats.emittedBytes += estimatedNumBytes;
    currentRunningCount.bytesCount += estimatedNumBytes;

    nameNamespacePairToStreamStats.put(nameNamespace, currStats);
  }

  /**
   * There are several assumptions here:
   * <p>
   * - Assume the estimate is a whole number and not a sum i.e. each estimate replaces the previous
   * estimate.
   * <p>
   * - Sources cannot emit both STREAM and SYNC estimates in a same sync. Error out if this happens.
   */
  @Override
  public void updateEstimates(final AirbyteEstimateTraceMessage estimate) {
    if (hasStreamEstimates.isEmpty()) {
      hasStreamEstimates = Optional.of(estimate.getType() == STREAM);
    }

    switch (estimate.getType()) {
      case STREAM -> {
        Preconditions.checkArgument(hasStreamEstimates.get(), "STREAM and SYNC estimates should not be emitted in the same sync.");

        final var nameNamespace = new AirbyteStreamNameNamespacePair(estimate.getName(), estimate.getNamespace());
        final var currStats = nameNamespacePairToStreamStats.getOrDefault(nameNamespace, new StreamStats());
        currStats.estimatedRecords = estimate.getRowEstimate();
        currStats.estimatedBytes = estimate.getByteEstimate();
        nameNamespacePairToStreamStats.put(nameNamespace, currStats);
      }
      case SYNC -> {
        Preconditions.checkArgument(!hasStreamEstimates.get(), "STREAM and SYNC estimates should not be emitted in the same sync.");

        totalBytesEstimatedSync = estimate.getByteEstimate();
        totalRecordsEstimatedSync = estimate.getRowEstimate();
      }
      default -> {
        // no op
      }
    }
  }

  /**
   * Update the stats count from the source state message.
   */
  @Override
  public void updateSourceStatesStats(final AirbyteStateMessage stateMessage) {
    final LocalDateTime timeEmittedStateMessage = LocalDateTime.now();
    stateMetricsTracker.incrementTotalSourceEmittedStateMessages();
    stateMetricsTracker.updateMaxAndMeanSecondsToReceiveStateMessage(timeEmittedStateMessage);
    stateMetricsTracker.setLastStateMessageReceivedAt(timeEmittedStateMessage);

    final int stateHash = getStateHashCode(stateMessage);
    try {
      if (!unreliableCommittedCounts) {
        stateDeltaTracker.addState(stateHash, streamToRunningCount);
      }
      if (!unreliableStateTimingMetrics) {
        stateMetricsTracker.addState(stateMessage, stateHash, timeEmittedStateMessage);
      }
    } catch (final StateDeltaTracker.StateDeltaTrackerException e) {
      log.warn("The message tracker encountered an issue that prevents committed record counts from being reliably computed.");
      log.warn("This only impacts metadata and does not indicate a problem with actual sync data.");
      log.warn(e.getMessage(), e);
      unreliableCommittedCounts = true;
    } catch (final StateMetricsTracker.StateMetricsTrackerOomException e) {
      log.warn("The StateMetricsTracker encountered an out of memory error that prevents new state metrics from being recorded");
      log.warn("This only affects metrics and does not indicate a problem with actual sync data.");
      unreliableStateTimingMetrics = true;
    }

    streamToRunningCount.clear();
  }

  /**
   * Update the stats count from the source state message.
   */
  @Override
  public void updateDestinationStateStats(final AirbyteStateMessage stateMessage) {
    final LocalDateTime timeCommitted = LocalDateTime.now();
    stateMetricsTracker.incrementTotalDestinationEmittedStateMessages();

    final int stateHash = getStateHashCode(stateMessage);
    try {
      if (!unreliableCommittedCounts) {
        stateDeltaTracker.commitStateHash(stateHash);
      }
    } catch (final StateDeltaTracker.StateDeltaTrackerException e) {
      log.warn("The message tracker encountered an issue that prevents committed record counts from being reliably computed. "
          + "This only impacts metadata and does not indicate a problem with actual sync data.", e);
      unreliableCommittedCounts = true;
    }

    try {
      if (!unreliableStateTimingMetrics) {
        stateMetricsTracker.updateStates(stateMessage, stateHash, timeCommitted);
      }
    } catch (final StateMetricsTrackerNoStateMatchException e) {
      log.warn("The state message tracker was unable to match the destination state message to a corresponding source state message."
          + "This only impacts metrics and does not indicate a problem with actual sync data.", e);
      unreliableStateTimingMetrics = true;
    }
  }

  /**
   * Swap out stream indices for stream names and return total records emitted by stream.
   */
  @Override
  public Map<AirbyteStreamNameNamespacePair, Long> getStreamToEmittedRecords() {
    return nameNamespacePairToStreamStats.entrySet().stream().collect(Collectors.toMap(
        Entry::getKey, entry -> entry.getValue().emittedRecords));
  }

  /**
   * Swap out stream indices for stream names and return total records estimated by stream.
   */
  @Override
  public Map<AirbyteStreamNameNamespacePair, Long> getStreamToEstimatedRecords() {
    return nameNamespacePairToStreamStats.entrySet().stream().collect(
        Collectors.toMap(
            Entry::getKey,
            entry -> entry.getValue().estimatedRecords));
  }

  /**
   * Swap out stream indices for stream names and return total bytes emitted by stream.
   */
  @Override
  public Map<AirbyteStreamNameNamespacePair, Long> getStreamToEmittedBytes() {
    return nameNamespacePairToStreamStats.entrySet().stream().collect(Collectors.toMap(
        Entry::getKey,
        entry -> entry.getValue().emittedBytes));
  }

  /**
   * Swap out stream indices for stream names and return total bytes estimated by stream.
   */
  @Override
  public Map<AirbyteStreamNameNamespacePair, Long> getStreamToEstimatedBytes() {
    return nameNamespacePairToStreamStats.entrySet().stream().collect(
        Collectors.toMap(
            Entry::getKey,
            entry -> entry.getValue().estimatedBytes));
  }

  /**
   * Compute sum of emitted record counts across all streams.
   */
  @Override
  public long getTotalRecordsEmitted() {
    return nameNamespacePairToStreamStats.values().stream()
        .map(stats -> stats.emittedRecords)
        .reduce(0L, Long::sum);
  }

  /**
   * Compute sum of estimated record counts across all streams.
   */
  @Override
  public long getTotalRecordsEstimated() {
    if (!nameNamespacePairToStreamStats.isEmpty()) {
      return nameNamespacePairToStreamStats.values().stream()
          .map(e -> e.estimatedRecords)
          .reduce(0L, Long::sum);
    }

    return totalRecordsEstimatedSync;
  }

  /**
   * Compute sum of emitted bytes across all streams.
   */
  @Override
  public long getTotalBytesEmitted() {
    return nameNamespacePairToStreamStats.values().stream()
        .map(e -> e.emittedBytes)
        .reduce(0L, Long::sum);
  }

  /**
   * Compute sum of estimated bytes across all streams.
   */
  @Override
  public long getTotalBytesEstimated() {
    if (!nameNamespacePairToStreamStats.isEmpty()) {
      return nameNamespacePairToStreamStats.values().stream()
          .map(e -> e.estimatedBytes)
          .reduce(0L, Long::sum);
    }

    return totalBytesEstimatedSync;
  }

  /**
   * Fetch committed stream index to record count from the {@link StateDeltaTracker}. Then, swap out
   * stream indices for stream names. If the delta tracker has exceeded its capacity, return empty
   * because committed record counts cannot be reliably computed.
   */
  @Override
  public Optional<Map<AirbyteStreamNameNamespacePair, Long>> getStreamToCommittedBytes() {
    if (unreliableCommittedCounts) {
      return Optional.empty();
    }
    final Map<Short, StatsCounters> streamIndexToCommittedStats = stateDeltaTracker.getStreamToCommittedStats();
    return Optional.of(
        streamIndexToCommittedStats.entrySet().stream().collect(
            Collectors.toMap(entry -> nameNamespacePairToIndex.inverse().get(entry.getKey()), e -> e.getValue().bytesCount)));
  }

  /**
   * Fetch committed stream index to record count from the {@link StateDeltaTracker}. Then, swap out
   * stream indices for stream names. If the delta tracker has exceeded its capacity, return empty
   * because committed record counts cannot be reliably computed.
   */
  @Override
  public Optional<Map<AirbyteStreamNameNamespacePair, Long>> getStreamToCommittedRecords() {
    if (unreliableCommittedCounts) {
      return Optional.empty();
    }
    final Map<Short, StatsCounters> streamIndexToCommittedStats = stateDeltaTracker.getStreamToCommittedStats();
    return Optional.of(
        streamIndexToCommittedStats.entrySet().stream().collect(
            Collectors.toMap(entry -> nameNamespacePairToIndex.inverse().get(entry.getKey()), e -> e.getValue().recordCount)));
  }

  /**
   * Compute sum of committed bytes counts across all streams. If the delta tracker has exceeded its
   * capacity, return empty because committed bytes counts cannot be reliably computed.
   */
  @Override
  public Optional<Long> getTotalBytesCommitted() {
    if (unreliableCommittedCounts) {
      return Optional.empty();
    }
    return Optional.of(stateDeltaTracker.getStreamToCommittedStats().values().stream().map(StatsCounters::getBytesCount).reduce(0L, Long::sum));
  }

  /**
   * Compute sum of committed record counts across all streams. If the delta tracker has exceeded its
   * capacity, return empty because committed record counts cannot be reliably computed.
   */
  @Override
  public Optional<Long> getTotalRecordsCommitted() {
    if (unreliableCommittedCounts) {
      return Optional.empty();
    }
    return Optional.of(stateDeltaTracker.getStreamToCommittedStats().values().stream().map(StatsCounters::getRecordCount).reduce(0L, Long::sum));
  }

  @Override
  public Long getTotalSourceStateMessagesEmitted() {
    return stateMetricsTracker.getTotalSourceStateMessageEmitted();
  }

  @Override
  public Long getTotalDestinationStateMessagesEmitted() {
    return stateMetricsTracker.getTotalDestinationStateMessageEmitted();
  }

  @Override
  public Long getMaxSecondsToReceiveSourceStateMessage() {
    return stateMetricsTracker.getMaxSecondsToReceiveSourceStateMessage();
  }

  @Override
  public Long getMeanSecondsToReceiveSourceStateMessage() {
    return stateMetricsTracker.getMeanSecondsToReceiveSourceStateMessage();
  }

  /**
   * Return the max number of seconds between a state message being emitted and committed. Returns
   * Optional.empty() if the stats are deemed unreliable.
   */
  @Override
  public Optional<Long> getMaxSecondsBetweenStateMessageEmittedAndCommitted() {
    if (unreliableStateTimingMetrics) {
      return Optional.empty();
    }

    return Optional.of(stateMetricsTracker.getMaxSecondsBetweenStateMessageEmittedAndCommitted());
  }

  /**
   * Return the average number of seconds between a state message being emitted and committed. Returns
   * Optional.empty() if the stats are deemed unreliable.
   */
  @Override
  public Optional<Long> getMeanSecondsBetweenStateMessageEmittedAndCommitted() {
    if (unreliableStateTimingMetrics) {
      return Optional.empty();
    }

    return Optional.of(stateMetricsTracker.getMeanSecondsBetweenStateMessageEmittedAndCommitted());
  }

  @Override
  public Boolean getUnreliableStateTimingMetrics() {
    return unreliableStateTimingMetrics;
  }

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

  private short getStreamIndex(final AirbyteStreamNameNamespacePair pair) {
    if (!nameNamespacePairToIndex.containsKey(pair)) {
      nameNamespacePairToIndex.put(pair, nextStreamIndex);
      nextStreamIndex++;
    }
    return nameNamespacePairToIndex.get(pair);
  }

}
