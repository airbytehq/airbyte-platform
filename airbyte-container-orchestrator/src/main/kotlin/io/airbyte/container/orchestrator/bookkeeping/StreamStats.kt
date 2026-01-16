/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.bookkeeping

import java.math.BigDecimal

interface StreamStats {
  val bytesCommitted: Long
  val bytesEmitted: Long
  val bytesEstimated: Long?
  val bytesFilteredOut: Long
  val recordsCommitted: Long
  val recordsEmitted: Long
  val recordsEstimated: Long?
  val recordsFilteredOut: Long
  val recordsRejected: Long
  val additionalStats: Map<String, BigDecimal>
}

class StreamStatsView(
  private val streamStatsTracker: StreamStatsTracker,
  private val hasEstimatesError: Boolean,
) : StreamStats {
  override val bytesCommitted: Long
    get() = streamStatsTracker.streamStats.committedBytesCount.get()

  override val bytesEmitted: Long
    get() = streamStatsTracker.streamStats.emittedBytesCount.get()

  override val bytesEstimated: Long?
    get() =
      streamStatsTracker.streamStats.estimatedBytesCount
        .get()
        .takeIf { !hasEstimatesError }

  override val bytesFilteredOut: Long
    get() = streamStatsTracker.streamStats.filteredOutBytesCount.get()

  override val recordsCommitted: Long
    get() = streamStatsTracker.streamStats.committedRecordsCount.get()

  override val recordsEmitted: Long
    get() = streamStatsTracker.streamStats.emittedRecordsCount.get()

  override val recordsEstimated: Long?
    get() =
      streamStatsTracker.streamStats.estimatedRecordsCount
        .get()
        .takeIf { !hasEstimatesError }

  override val recordsRejected: Long
    get() = streamStatsTracker.streamStats.rejectedRecordsCount.get()

  override val recordsFilteredOut: Long
    get() = streamStatsTracker.streamStats.filteredOutRecords.get()

  override val additionalStats: Map<String, BigDecimal>
    get() = streamStatsTracker.streamStats.additionalStats
}
