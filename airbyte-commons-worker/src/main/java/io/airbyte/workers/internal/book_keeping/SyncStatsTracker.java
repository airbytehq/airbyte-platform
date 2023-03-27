/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.book_keeping;

import io.airbyte.protocol.models.AirbyteEstimateTraceMessage;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.airbyte.protocol.models.AirbyteStreamNameNamespacePair;
import java.util.Map;
import java.util.Optional;

/**
 * Track stats during a sync.
 */
public interface SyncStatsTracker {

  /**
   * Update the stats count with data from recordMessage.
   */
  void updateStats(final AirbyteRecordMessage recordMessage);

  /**
   * There are several assumptions here:
   * <p>
   * - Assume the estimate is a whole number and not a sum i.e. each estimate replaces the previous
   * estimate.
   * <p>
   * - Sources cannot emit both STREAM and SYNC estimates in a same sync. Error out if this happens.
   */
  void updateEstimates(final AirbyteEstimateTraceMessage estimate);

  /**
   * Update the stats count from the source state message.
   */
  void updateSourceStatesStats(final AirbyteStateMessage stateMessage);

  /**
   * Update the stats count from the source state message.
   */
  void updateDestinationStateStats(final AirbyteStateMessage stateMessage);

  /**
   * Get the per-stream committed record count.
   *
   * @return returns a map of committed record count by stream name. If committed record counts cannot
   *         be computed, empty.
   */
  Optional<Map<AirbyteStreamNameNamespacePair, Long>> getStreamToCommittedRecords();

  /**
   * Get the per-stream emitted record count. This includes messages that were emitted by the source,
   * but never committed by the destination.
   *
   * @return returns a map of emitted record count by stream name.
   */
  Map<AirbyteStreamNameNamespacePair, Long> getStreamToEmittedRecords();

  /**
   * Get the per-stream estimated record count provided by
   * {@link io.airbyte.protocol.models.AirbyteEstimateTraceMessage}.
   *
   * @return returns a map of estimated record count by stream name.
   */
  Map<AirbyteStreamNameNamespacePair, Long> getStreamToEstimatedRecords();

  /**
   * Get the per-stream emitted byte count. This includes messages that were emitted by the source,
   * but never committed by the destination.
   *
   * @return returns a map of emitted record count by stream name.
   */
  Map<AirbyteStreamNameNamespacePair, Long> getStreamToEmittedBytes();

  /**
   * Get the per-stream estimated byte count provided by
   * {@link io.airbyte.protocol.models.AirbyteEstimateTraceMessage}.
   *
   * @return returns a map of estimated bytes by stream name.
   */
  Map<AirbyteStreamNameNamespacePair, Long> getStreamToEstimatedBytes();

  /**
   * Get the overall emitted record count. This includes messages that were emitted by the source, but
   * never committed by the destination.
   *
   * @return returns the total count of emitted records across all streams.
   */
  long getTotalRecordsEmitted();

  /**
   * Get the overall estimated record count.
   *
   * @return returns the total count of estimated records across all streams.
   */
  long getTotalRecordsEstimated();

  /**
   * Get the overall emitted bytes. This includes messages that were emitted by the source, but never
   * committed by the destination.
   *
   * @return returns the total emitted bytes across all streams.
   */
  long getTotalBytesEmitted();

  /**
   * Get the overall estimated bytes.
   *
   * @return returns the total count of estimated bytes across all streams.
   */
  long getTotalBytesEstimated();

  /**
   * Get the overall committed record count.
   *
   * @return returns the total count of committed records across all streams. If total committed
   *         record count cannot be computed, empty.
   */
  Optional<Long> getTotalRecordsCommitted();

  /**
   * Get the count of state messages emitted from the source connector.
   *
   * @return returns the total count of state messages emitted from the source.
   */
  Long getTotalSourceStateMessagesEmitted();

  Long getTotalDestinationStateMessagesEmitted();

  Long getMaxSecondsToReceiveSourceStateMessage();

  Long getMeanSecondsToReceiveSourceStateMessage();

  Optional<Long> getMaxSecondsBetweenStateMessageEmittedAndCommitted();

  Optional<Long> getMeanSecondsBetweenStateMessageEmittedAndCommitted();

  Boolean getUnreliableStateTimingMetrics();

}
