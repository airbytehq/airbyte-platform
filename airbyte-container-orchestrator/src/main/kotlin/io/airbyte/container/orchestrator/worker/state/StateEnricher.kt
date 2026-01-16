/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.state

import io.airbyte.container.orchestrator.bookkeeping.ParallelStreamStatsTracker
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteStateStats
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton

/**
 * Enriches the state message with operational data used internally in the orchestrator and
 * downstream by the destination.
 *
 * Only performed in Orchestrator-mode.
 */
@Singleton
class StateEnricher(
  private val statsTracker: ParallelStreamStatsTracker,
) {
  private val log = KotlinLogging.logger {}

  /**
   * Enforces necessary state "watermarks" used by destination to know when to ack states.
   *
   * Adds IDs for internal state tracking.
   */
  fun enrich(message: AirbyteMessage): AirbyteMessage {
    // filtering in the parent as-is is currently unergonomic so we do so here
    if (message.type != AirbyteMessage.Type.STATE) {
      return message
    }

    // add source stats if not present for dest
    if (message.state.sourceStats == null) {
      val recordCount = statsTracker.getEmittedCountForCurrentState(message.state)

      val stats = AirbyteStateStats()
      if (recordCount == null) {
        log.warn { "No records detected for enriched STATE stats." }
      }
      stats.recordCount = recordCount?.toDouble() ?: 0.0
      message.state.sourceStats = stats
    }
    // decrement filtered from count so dest doesn't wait for records that aren't coming
    val filteredOut = statsTracker.getFilteredCountForCurrentState(message.state)
    if (filteredOut != null) {
      val emitted = message.state.sourceStats.recordCount
      require(filteredOut <= emitted) { "More records were filtered ($filteredOut) than emitted ($emitted)." }

      message.state.sourceStats.recordCount -= filteredOut
    }

    // lastly add an ID for internal bookkeeping
    return attachIdToStateMessageFromSource(message)
  }
}
