/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.services

import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.Job
import io.airbyte.config.JobConfigProxy
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.SyncMode
import io.airbyte.data.repositories.StreamStatsRepository
import io.airbyte.data.repositories.entities.StreamStats
import io.airbyte.persistence.job.JobPersistence
import jakarta.inject.Singleton
import java.math.BigDecimal

/**
 * Service for accessing stream-level statistics.
 */
@Singleton
class StreamStatsService(
  private val streamStatsRepository: StreamStatsRepository,
  private val jobPersistence: JobPersistence,
) {
  data class AggregatedStreamStats(
    val streamName: String,
    val streamNamespace: String?,
    val recordsEmitted: Long,
    val bytesEmitted: Long,
    val recordsCommitted: Long,
    val bytesCommitted: Long,
    val recordsRejected: Long,
    val additionalStats: Map<String, BigDecimal>,
    val wasBackfilled: Boolean = false,
    val wasResumed: Boolean = false,
  )

  /**
   * Get aggregated stats for all streams in a job across all attempts.
   *
   * @param jobId The job ID to fetch stats for
   * @return List of aggregated stats per stream
   */
  fun getAggregatedStatsForJob(jobId: Long): List<AggregatedStreamStats> {
    // 1. Get sync modes from job config
    val job = jobPersistence.getJob(jobId)
    val streamToSyncMode = getStreamsToSyncMode(job)

    // 2. Get raw stats from repository (includes metadata)
    val allStreamStats = streamStatsRepository.findByJobId(jobId)

    // 3. Group by stream and sort by attempt ID
    val statsByStream =
      allStreamStats
        .groupBy { StreamDescriptor().withName(it.streamName).withNamespace(it.streamNamespace) }
        .mapValues { (_, stats) -> stats.sortedBy { it.attemptId } }

    // 4. Aggregate each stream based on sync mode
    return statsByStream.mapNotNull { (descriptor, stats) ->
      streamToSyncMode[descriptor]?.let { syncMode ->
        aggregateStreamStats(syncMode, stats)
      }
    }
  }

  /**
   * Extract sync modes from job configuration.
   */
  private fun getStreamsToSyncMode(job: Job): Map<StreamDescriptor, SyncMode> {
    val configuredAirbyteStreams = extractStreams(job)
    return configuredAirbyteStreams.associate { configuredStream ->
      StreamDescriptor()
        .withName(configuredStream.stream.name)
        .withNamespace(configuredStream.stream.namespace) to configuredStream.syncMode
    }
  }

  private fun extractStreams(job: Job): List<ConfiguredAirbyteStream> {
    val configuredCatalog = JobConfigProxy(job.config).configuredCatalog
    return configuredCatalog?.streams ?: listOf()
  }

  /**
   * Aggregate stats for a single stream across multiple attempts.
   *
   * How stats are aggregated depends on the sync mode:
   * - Full refresh: Select stats from the last non-resumed attempt + all subsequent resumed attempts, then sum
   * - Incremental: Sum stats from all attempts
   */
  private fun aggregateStreamStats(
    syncMode: SyncMode,
    streamStats: List<StreamStats>,
  ): AggregatedStreamStats {
    require(streamStats.isNotEmpty()) { "streamStats must not be empty" }

    // Select which attempts to aggregate based on sync mode
    val statsToAggregate =
      when (syncMode) {
        SyncMode.FULL_REFRESH -> selectResumedStats(streamStats)
        SyncMode.INCREMENTAL -> streamStats
      }

    // Aggregate all stats in a single loop
    var recordsEmitted: Long = 0
    var bytesEmitted: Long = 0
    var recordsCommitted: Long = 0
    var bytesCommitted: Long = 0
    var recordsRejected: Long = 0
    val aggregatedAdditionalStats = mutableMapOf<String, BigDecimal>()

    for (stat in statsToAggregate) {
      // Sum standard stats
      recordsEmitted += stat.recordsEmitted ?: 0
      bytesEmitted += stat.bytesEmitted ?: 0
      recordsCommitted += stat.recordsCommitted ?: 0
      bytesCommitted += stat.bytesCommitted ?: 0
      recordsRejected += stat.recordsRejected ?: 0

      // Sum additional stats
      stat.additionalStats?.forEach { (key, value) ->
        if (value != null) {
          aggregatedAdditionalStats[key] = aggregatedAdditionalStats.getOrDefault(key, BigDecimal.ZERO) + value
        }
      }
    }

    // OR the metadata flags across all attempts (not just aggregated ones)
    var wasBackfilled = false
    var wasResumed = false
    for (stat in streamStats) {
      wasBackfilled = wasBackfilled || (stat.wasBackfilled ?: false)
      wasResumed = wasResumed || (stat.wasResumed ?: false)
    }

    return AggregatedStreamStats(
      streamName = streamStats.first().streamName,
      streamNamespace = streamStats.first().streamNamespace,
      recordsEmitted = recordsEmitted,
      bytesEmitted = bytesEmitted,
      recordsCommitted = recordsCommitted,
      bytesCommitted = bytesCommitted,
      recordsRejected = recordsRejected,
      additionalStats = aggregatedAdditionalStats,
      wasBackfilled = wasBackfilled,
      wasResumed = wasResumed,
    )
  }

  /**
   * For full refresh streams with resume, select the last non-resumed attempt + all subsequent attempts.
   * This implements the billing logic for resumable full refresh.
   */
  private fun selectResumedStats(streamStats: List<StreamStats>): List<StreamStats> {
    var i = streamStats.size - 1
    // Work backwards to find the last non-resumed attempt
    while (i > 0 && streamStats[i].wasResumed == true) {
      i--
    }
    // Include that attempt and all subsequent ones
    return streamStats.subList(i, streamStats.size)
  }
}
