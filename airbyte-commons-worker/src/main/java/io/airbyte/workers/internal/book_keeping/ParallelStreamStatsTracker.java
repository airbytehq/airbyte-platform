/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.book_keeping;

import io.airbyte.config.StreamSyncStats;
import io.airbyte.config.SyncStats;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.protocol.models.AirbyteEstimateTraceMessage;
import io.airbyte.protocol.models.AirbyteEstimateTraceMessage.Type;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.airbyte.protocol.models.AirbyteStreamNameNamespacePair;
import io.airbyte.workers.internal.book_keeping.StreamStatsTracker.StreamStatsCounters;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Track Stats for a specific sync.
 * <p>
 * Tracking is done at stream level and resilient to stream being processed in parallel.
 */
@Slf4j
public class ParallelStreamStatsTracker implements SyncStatsTracker {

  record SyncStatsCounters(AtomicLong estimatedRecordsCount, AtomicLong estimatedBytesCount) {}

  private final MetricClient metricClient;
  private final Map<AirbyteStreamNameNamespacePair, StreamStatsTracker> streamTrackers;
  private final SyncStatsCounters syncStatsCounters;
  private Optional<Type> expectedEstimateType;
  private volatile boolean hasEstimatesErrors;

  public ParallelStreamStatsTracker(final MetricClient metricClient) {
    this.metricClient = metricClient;
    this.streamTrackers = new ConcurrentHashMap<>();
    this.syncStatsCounters = new SyncStatsCounters(new AtomicLong(), new AtomicLong());
    this.expectedEstimateType = Optional.empty();
    this.hasEstimatesErrors = false;
  }

  @Override
  public void updateStats(final AirbyteRecordMessage recordMessage) {
    final AirbyteStreamNameNamespacePair nameNamespacePair = getNameNamespacePair(recordMessage);
    final StreamStatsTracker streamStatsTracker = getOrCreateStreamStatsTracker(nameNamespacePair);
    streamStatsTracker.trackRecord(recordMessage);
  }

  @Override
  public void updateEstimates(final AirbyteEstimateTraceMessage estimate) {
    if (hasEstimatesErrors) {
      return;
    }

    if (expectedEstimateType.isEmpty()) {
      expectedEstimateType = Optional.of(estimate.getType());
    }

    if (!expectedEstimateType.get().equals(estimate.getType())) {
      log.info("STREAM and SYNC estimates should not be emitted in the same sync.");
      hasEstimatesErrors = true;
      return;
    }

    switch (estimate.getType()) {
      case STREAM -> {
        final AirbyteStreamNameNamespacePair nameNamespacePair = getNameNamespacePair(estimate);
        final StreamStatsTracker streamStatsTracker = getOrCreateStreamStatsTracker(nameNamespacePair);
        streamStatsTracker.trackEstimates(estimate);
      }
      case SYNC -> {
        syncStatsCounters.estimatedBytesCount.set(estimate.getByteEstimate());
        syncStatsCounters.estimatedRecordsCount.set(estimate.getRowEstimate());
      }
      default -> {
        // No-op
      }
    }
  }

  @Override
  public void updateSourceStatesStats(final AirbyteStateMessage stateMessage) {
    final AirbyteStreamNameNamespacePair nameNamespacePair = getNameNamespacePair(stateMessage);
    final StreamStatsTracker streamStatsTracker = getOrCreateStreamStatsTracker(nameNamespacePair);
    streamStatsTracker.trackStateFromSource(stateMessage);
  }

  @Override
  public void updateDestinationStateStats(final AirbyteStateMessage stateMessage) {
    final AirbyteStreamNameNamespacePair nameNamespacePair = getNameNamespacePair(stateMessage);
    final StreamStatsTracker streamStatsTracker = getOrCreateStreamStatsTracker(nameNamespacePair);
    streamStatsTracker.trackStateFromDestination(stateMessage);
  }

  /**
   * Return SyncStats for the sync. SyncStats is the sum of the stats of all the streams.
   *
   * @param hasReplicationCompleted defines whether a stream has completed. If the stream has
   *        completed, emitted counts/bytes will be used as committed counts/bytes.
   */
  public SyncStats getTotalStats(final boolean hasReplicationCompleted) {
    final SyncStats syncStats = new SyncStats();
    for (final var streamSyncStats : getAllStreamSyncStats(hasReplicationCompleted)) {
      final var streamStats = streamSyncStats.getStats();
      syncStats.setBytesCommitted(nullSafeSum(syncStats.getBytesCommitted(), streamStats.getBytesCommitted()));
      syncStats.setRecordsCommitted(nullSafeSum(syncStats.getRecordsCommitted(), streamStats.getRecordsCommitted()));
      syncStats.setBytesEmitted(nullSafeSum(syncStats.getBytesEmitted(), streamStats.getBytesEmitted()));
      syncStats.setRecordsEmitted(nullSafeSum(syncStats.getRecordsEmitted(), streamStats.getRecordsEmitted()));
      syncStats.setEstimatedBytes(nullSafeSum(syncStats.getEstimatedBytes(), streamStats.getEstimatedBytes()));
      syncStats.setEstimatedRecords(nullSafeSum(syncStats.getEstimatedRecords(), streamStats.getEstimatedRecords()));
    }

    // We have been tracking SyncEstimates, report back the estimates.
    if (!hasEstimatesErrors && expectedEstimateType.map(Type.SYNC::equals).orElse(false)) {
      syncStats.setEstimatedBytes(syncStatsCounters.estimatedBytesCount.get());
      syncStats.setEstimatedRecords(syncStatsCounters.estimatedRecordsCount.get());
    }
    return syncStats;
  }

  /**
   * Sum lhs and rhs with null checks.
   */
  private Long nullSafeSum(final Long lhs, final Long rhs) {
    if (lhs == null) {
      return rhs;
    } else if (rhs == null) {
      return lhs;
    } else {
      return lhs + rhs;
    }
  }

  /**
   * Return all the StreamSyncStats for the sync.
   *
   * @param hasReplicationCompleted defines whether a stream has completed. If the stream has
   *        completed, emitted counts/bytes will be used as committed counts/bytes.
   */
  public List<StreamSyncStats> getAllStreamSyncStats(final boolean hasReplicationCompleted) {
    final List<StreamSyncStats> streamSyncStatsList = new ArrayList<>();
    for (final var streamTracker : streamTrackers.values()) {
      final StreamStatsCounters streamStats = streamTracker.getStreamStats();
      streamSyncStatsList.add(new StreamSyncStats()
          .withStreamName(streamTracker.getNameNamespacePair().getName())
          .withStreamNamespace(streamTracker.getNameNamespacePair().getNamespace())
          .withStats(new SyncStats()
              .withBytesCommitted(hasReplicationCompleted ? streamStats.emittedBytesCount().get() : streamStats.committedBytesCount().get())
              .withRecordsCommitted(hasReplicationCompleted ? streamStats.emittedRecordsCount().get() : streamStats.committedRecordsCount().get())
              .withBytesEmitted(streamStats.emittedBytesCount().get())
              .withRecordsEmitted(streamStats.emittedRecordsCount().get())
              .withEstimatedBytes(!hasEstimatesErrors ? streamStats.estimatedBytesCount().get() : null)
              .withEstimatedRecords(!hasEstimatesErrors ? streamStats.estimatedRecordsCount().get() : null)));
    }
    return streamSyncStatsList;
  }

  /**
   * Get the StreamStatsTrocker for a given stream. If this tracker doesn't exist, create it.
   */
  private StreamStatsTracker getOrCreateStreamStatsTracker(final AirbyteStreamNameNamespacePair nameNamespacePair) {
    StreamStatsTracker streamStatsTracker = streamTrackers.get(nameNamespacePair);
    if (streamStatsTracker != null) {
      return streamStatsTracker;
    }

    // We want to avoid multiple threads trying to create a new StreamStatsTracker.
    // This operation should be fairly rare, once per stream, so the synchronized block shouldn't cause
    // too much contention.
    synchronized (this) {
      // Making sure the stream hasn't been created since the previous check.
      streamStatsTracker = streamTrackers.get(nameNamespacePair);
      if (streamStatsTracker != null) {
        return streamStatsTracker;
      }

      streamStatsTracker = new StreamStatsTracker(nameNamespacePair, metricClient);
      streamTrackers.put(nameNamespacePair, streamStatsTracker);
    }
    return streamStatsTracker;
  }

  private AirbyteStreamNameNamespacePair getNameNamespacePair(final AirbyteEstimateTraceMessage estimateMessage) {
    return new AirbyteStreamNameNamespacePair(estimateMessage.getName(), estimateMessage.getNamespace());
  }

  private AirbyteStreamNameNamespacePair getNameNamespacePair(final AirbyteRecordMessage recordMessage) {
    return AirbyteStreamNameNamespacePair.fromRecordMessage(recordMessage);
  }

  private AirbyteStreamNameNamespacePair getNameNamespacePair(final AirbyteStateMessage stateMessage) {
    if (stateMessage.getStream() != null) {
      final var streamDescriptor = stateMessage.getStream().getStreamDescriptor();
      return new AirbyteStreamNameNamespacePair(streamDescriptor.getName(), streamDescriptor.getNamespace());
    } else {
      return new AirbyteStreamNameNamespacePair(null, null);
    }
  }

  // Helper for the flurry of accessors.
  // TODO The interface should be updated to handle the top level objects rather than exposing each
  // field.
  private Optional<Map<AirbyteStreamNameNamespacePair, Long>> toMap(final Function<StreamStatsTracker, Long> valueReader) {
    return Optional.of(streamTrackers.values().stream()
        .collect(Collectors.toMap(StreamStatsTracker::getNameNamespacePair, valueReader::apply)));
  }

  @Override
  public Optional<Map<AirbyteStreamNameNamespacePair, Long>> getStreamToCommittedBytes() {
    return toMap(s -> s.getStreamStats().committedBytesCount().get());
  }

  @Override
  public Optional<Map<AirbyteStreamNameNamespacePair, Long>> getStreamToCommittedRecords() {
    return toMap(s -> s.getStreamStats().committedRecordsCount().get());
  }

  @Override
  public Map<AirbyteStreamNameNamespacePair, Long> getStreamToEmittedRecords() {
    return toMap(s -> s.getStreamStats().emittedRecordsCount().get()).get();
  }

  @Override
  public Map<AirbyteStreamNameNamespacePair, Long> getStreamToEstimatedRecords() {
    return !hasEstimatesErrors ? toMap(s -> s.getStreamStats().estimatedRecordsCount().get()).get() : Map.of();
  }

  @Override
  public Map<AirbyteStreamNameNamespacePair, Long> getStreamToEmittedBytes() {
    return toMap(s -> s.getStreamStats().emittedBytesCount().get()).get();
  }

  @Override
  public Map<AirbyteStreamNameNamespacePair, Long> getStreamToEstimatedBytes() {
    return !hasEstimatesErrors ? toMap(s -> s.getStreamStats().estimatedBytesCount().get()).get() : Map.of();
  }

  @Override
  public long getTotalRecordsEmitted() {
    return Objects.requireNonNullElse(getTotalStats(false).getRecordsEmitted(), 0L);
  }

  @Override
  public long getTotalRecordsEstimated() {
    final Long estimatedRecords = getTotalStats(false).getEstimatedRecords();
    return estimatedRecords != null ? estimatedRecords : 0L;
  }

  @Override
  public long getTotalBytesEmitted() {
    return Objects.requireNonNullElse(getTotalStats(false).getBytesEmitted(), 0L);
  }

  @Override
  public long getTotalBytesEstimated() {
    final Long estimatedBytes = getTotalStats(false).getEstimatedBytes();
    return estimatedBytes != null ? estimatedBytes : 0L;
  }

  @Override
  public Optional<Long> getTotalBytesCommitted() {
    return Optional.ofNullable(getTotalStats(false).getBytesCommitted());
  }

  @Override
  public Optional<Long> getTotalRecordsCommitted() {
    return Optional.ofNullable(getTotalStats(false).getRecordsCommitted());
  }

  @Override
  public Long getTotalSourceStateMessagesEmitted() {
    return streamTrackers.values().stream().map(s -> s.getStreamStats().sourceStateCount().get()).reduce(Long::sum).orElse(0L);
  }

  @Override
  public Long getTotalDestinationStateMessagesEmitted() {
    return streamTrackers.values().stream().map(s -> s.getStreamStats().destinationStateCount().get()).reduce(Long::sum).orElse(0L);
  }

  @Override
  public Long getMaxSecondsToReceiveSourceStateMessage() {
    return streamTrackers.values().stream().map(s -> s.getStreamStats().destinationStateCount().get()).reduce(Long::max).orElse(0L);
  }

  @Override
  public Long getMeanSecondsToReceiveSourceStateMessage() {
    return computeMean(s -> (double) s.getStreamStats().maxSecondsToReceiveState().get(), s -> s.getStreamStats().sourceStateCount().get() - 1);
  }

  @Override
  public Optional<Long> getMaxSecondsBetweenStateMessageEmittedAndCommitted() {
    // We drop counts if we have state operation errors since the metric with be noise under those
    // conditions
    if (hasSourceStateErrors()) {
      return Optional.empty();
    }

    final long max =
        streamTrackers.values().stream().map(s -> s.getStreamStats().maxSecondsBetweenStateEmittedAndCommitted().get()).reduce(Long::max).orElse(0L);
    return Optional.of(max);
  }

  @Override
  public Optional<Long> getMeanSecondsBetweenStateMessageEmittedAndCommitted() {
    // We drop counts if we have state operation errors since the metric with be noise under those
    // conditions
    if (hasSourceStateErrors()) {
      return Optional.empty();
    }

    final Long mean = computeMean(s -> s.getStreamStats().meanSecondsBetweenStateEmittedAndCommitted().get(),
        s -> s.getStreamStats().destinationStateCount().get());
    return Optional.of(mean);
  }

  @Override
  public Boolean getUnreliableStateTimingMetrics() {
    return hasSourceStateErrors();
  }

  /**
   * Helper function to compute some global average from the per stream averages.
   *
   * @param getMean how to get the local average from a StreamStatsTracker
   * @param getCount how to get the local total count related to that average from a
   *        StreamStatsTracker
   * @return the global average
   */
  private Long computeMean(final Function<StreamStatsTracker, Double> getMean, final Function<StreamStatsTracker, Long> getCount) {
    double currentAverage = 0;
    double currentTotal = 0;
    for (final var streamTracker : streamTrackers.values()) {
      final double stateCount = getCount.apply(streamTracker);
      final double newTotal = currentTotal + stateCount;
      currentAverage = ((currentAverage * currentTotal) + (getMean.apply(streamTracker) * stateCount)) / newTotal;
    }
    return (long) currentAverage;

  }

  /**
   * True if any streams has an unreliable state operation flag set to true.
   */
  private boolean hasSourceStateErrors() {
    return streamTrackers.values().stream().anyMatch(s -> s.getStreamStats().unreliableStateOperations().get());
  }

}
