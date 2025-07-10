/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.bookkeeping

import io.airbyte.container.orchestrator.worker.context.ReplicationInputFeatureFlagReader
import io.airbyte.protocol.models.v0.AirbyteEstimateTraceMessage
import io.airbyte.protocol.models.v0.AirbyteRecordMessage
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStreamNameNamespacePair

/**
 * Track stats during a sync.
 */
interface SyncStatsTracker {
  fun updateFilteredOutRecordsStats(recordMessage: AirbyteRecordMessage)

  /**
   * Update the stats count with data from recordMessage.
   */
  fun updateStats(recordMessage: AirbyteRecordMessage)

  fun updateStatsFromDestination(recordMessage: AirbyteRecordMessage)

  /**
   * There are several assumptions here:
   *
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
  fun updateSourceStatesStats(stateMessage: AirbyteStateMessage)

  /**
   * Update the stats count from the source state message.
   */
  fun updateDestinationStateStats(stateMessage: AirbyteStateMessage)

  fun getStats(): Map<AirbyteStreamNameNamespacePair, StreamStats>

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

  fun setReplicationFeatureFlagReader(replicationInputFeatureFlagReader: ReplicationInputFeatureFlagReader)

  fun endOfReplication(completedSuccessfully: Boolean)
}
