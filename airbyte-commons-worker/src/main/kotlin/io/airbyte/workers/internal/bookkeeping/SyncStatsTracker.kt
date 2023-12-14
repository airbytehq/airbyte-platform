/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.workers.internal.bookkeeping

import io.airbyte.protocol.models.AirbyteEstimateTraceMessage
import io.airbyte.protocol.models.AirbyteRecordMessage
import io.airbyte.protocol.models.AirbyteStateMessage
import io.airbyte.protocol.models.AirbyteStreamNameNamespacePair

/**
 * Track stats during a sync.
 */
interface SyncStatsTracker {
  /**
   * Update the stats count with data from recordMessage.
   */
  fun updateStats(recordMessage: AirbyteRecordMessage)

  /**
   * There are several assumptions here:
   *
   *f
   * - Assume the estimate is a whole number and not a sum i.e. each estimate replaces the previous
   * estimate.
   *
   *
   * - Sources cannot emit both STREAM and SYNC estimates in a same sync. Error out if this happens.
   */
  fun updateEstimates(estimate: AirbyteEstimateTraceMessage)

  /**
   * Update the stats count from the source state message.
   */
  fun updateSourceStatesStats(
    stateMessage: AirbyteStateMessage,
    trackCommittedStatsWhenUsingGlobalState: Boolean,
  )

  /**
   * Update the stats count from the source state message.
   */
  fun updateDestinationStateStats(
    stateMessage: AirbyteStateMessage,
    trackCommittedStatsWhenUsingGlobalState: Boolean,
  )

  /**
   * Get the per-stream committed bytes count.
   *
   * @return returns a map of committed bytes count by stream name. If committed bytes counts cannot
   * be computed, empty.
   */
  fun getStreamToCommittedBytes(): Map<AirbyteStreamNameNamespacePair, Long>

  /**
   * Get the per-stream committed record count.
   *
   * @return returns a map of committed record count by stream name. If committed record counts cannot
   * be computed, empty.
   */
  fun getStreamToCommittedRecords(): Map<AirbyteStreamNameNamespacePair, Long>

  /**
   * Get the per-stream emitted record count. This includes messages that were emitted by the source,
   * but never committed by the destination.
   *
   * @return returns a map of emitted record count by stream name.
   */
  fun getStreamToEmittedRecords(): Map<AirbyteStreamNameNamespacePair, Long>

  /**
   * Get the per-stream estimated record count provided by
   * [io.airbyte.protocol.models.AirbyteEstimateTraceMessage].
   *
   * @return returns a map of estimated record count by stream name.
   */
  fun getStreamToEstimatedRecords(): Map<AirbyteStreamNameNamespacePair, Long>

  /**
   * Get the per-stream emitted byte count. This includes messages that were emitted by the source,
   * but never committed by the destination.
   *
   * @return returns a map of emitted record count by stream name.
   */
  fun getStreamToEmittedBytes(): Map<AirbyteStreamNameNamespacePair, Long>

  /**
   * Get the per-stream estimated byte count provided by
   * [io.airbyte.protocol.models.AirbyteEstimateTraceMessage].
   *
   * @return returns a map of estimated bytes by stream name.
   */
  fun getStreamToEstimatedBytes(): Map<AirbyteStreamNameNamespacePair, Long>

  /**
   * Get the overall emitted record count. This includes messages that were emitted by the source, but
   * never committed by the destination.
   *
   * @return returns the total count of emitted records across all streams.
   */
  fun getTotalRecordsEmitted(): Long

  /**
   * Get the overall estimated record count.
   *
   * @return returns the total count of estimated records across all streams.
   */
  fun getTotalRecordsEstimated(): Long

  /**
   * Get the overall emitted bytes. This includes messages that were emitted by the source, but never
   * committed by the destination.
   *
   * @return returns the total emitted bytes across all streams.
   */
  fun getTotalBytesEmitted(): Long

  /**
   * Get the overall estimated bytes.
   *
   * @return returns the total count of estimated bytes across all streams.
   */
  fun getTotalBytesEstimated(): Long

  /**
   * Get the overall committed bytes count.
   *
   * @return returns the total count of committed bytes across all streams. If total committed bytes
   * count cannot be computed, empty.
   */
  fun getTotalBytesCommitted(): Long?

  /**
   * Get the overall committed record count.
   *
   * @return returns the total count of committed records across all streams. If total committed
   * record count cannot be computed, empty.
   */
  fun getTotalRecordsCommitted(): Long?

  /**
   * Get the count of state messages emitted from the source connector.
   *
   * @return returns the total count of state messages emitted from the source.
   */
  fun getTotalSourceStateMessagesEmitted(): Long

  fun getTotalDestinationStateMessagesEmitted(): Long

  fun getMaxSecondsToReceiveSourceStateMessage(): Long

  fun getMeanSecondsToReceiveSourceStateMessage(): Long

  fun getMaxSecondsBetweenStateMessageEmittedAndCommitted(): Long?

  fun getMeanSecondsBetweenStateMessageEmittedAndCommitted(): Long?

  fun getUnreliableStateTimingMetrics(): Boolean
}
