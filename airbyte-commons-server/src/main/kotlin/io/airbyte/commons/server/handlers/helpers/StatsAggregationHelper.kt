/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import io.airbyte.api.model.generated.AttemptRead
import io.airbyte.api.model.generated.AttemptStats
import io.airbyte.api.model.generated.AttemptStreamStats
import io.airbyte.api.model.generated.JobAggregatedStats
import io.airbyte.api.model.generated.JobWithAttemptsRead
import io.airbyte.api.model.generated.StreamStats
import io.airbyte.commons.server.converters.JobConverter
import io.airbyte.commons.server.handlers.JobHistoryHandler
import io.airbyte.commons.server.handlers.JobHistoryHandler.StreamNameAndNamespace
import io.airbyte.config.Job
import io.airbyte.config.StreamSyncStats
import io.airbyte.config.SyncMode
import io.airbyte.persistence.job.JobPersistence
import io.airbyte.persistence.job.JobPersistence.JobAttemptPair
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.validation.Valid
import java.io.IOException
import java.util.Optional
import java.util.function.Consumer
import java.util.function.Function
import java.util.stream.Collectors

/**
 * Helper class to aggregate stream stats. The class is meant to be used to aggregate stats for a
 * single stream across multiple attempts
 */
object StatsAggregationHelper {
  private val log = KotlinLogging.logger {}

  /**
   * WARNING! billing uses the stats that this method returns. Be careful when changing this method.
   *
   * Aggregates stream stats based on the given sync mode. The given stream stats should be a list of
   * stats for a single stream across multiple attempts. The list must be sorted by attempt id in
   * ascending order. The given sync mode should be the sync mode of the stream of which the stream
   * stats belong to.
   *
   * How stats are aggregated depends on the given sync mode
   *
   *  * Full refresh: the aggregated stats equals to the stats of the last element of the given
   * stream stats list
   *  * Incremental: the aggregated stats are the sum of the stats of all stream stats
   *
   *
   * @param syncMode stream sync mode
   * @param streamStats stream attempt stats. Should have at least one element
   * @return aggregated stats for the given stream
   */
  @JvmStatic
  fun getAggregatedStats(
    syncMode: SyncMode,
    streamStats: List<StreamSyncStats>,
  ): StreamStatsRecord =
    when (syncMode) {
      SyncMode.FULL_REFRESH ->
        getAggregatedStats(
          selectResumedStats(
            streamStats,
          ),
        )

      SyncMode.INCREMENTAL -> getAggregatedStats(streamStats)
      else -> throw IllegalArgumentException("Unknown sync mode: $syncMode")
    }

  private fun getAggregatedStats(streamStats: List<StreamSyncStats>): StreamStatsRecord {
    var recordsEmitted: Long = 0
    var bytesEmitted: Long = 0
    var recordsCommitted: Long = 0
    var bytesCommitted: Long = 0
    var recordsRejected: Long = 0

    for (streamStat in streamStats) {
      val syncStats = streamStat.stats
      recordsEmitted += if (syncStats.recordsEmitted == null) 0 else syncStats.recordsEmitted
      bytesEmitted += if (syncStats.bytesEmitted == null) 0 else syncStats.bytesEmitted
      recordsCommitted += if (syncStats.recordsCommitted == null) 0 else syncStats.recordsCommitted
      bytesCommitted += if (syncStats.bytesCommitted == null) 0 else syncStats.bytesCommitted
      recordsRejected += if (syncStats.recordsRejected == null) 0 else syncStats.recordsRejected
    }

    return StreamStatsRecord(
      streamStats.first().streamName,
      streamStats.first().streamNamespace,
      recordsEmitted,
      bytesEmitted,
      recordsCommitted,
      bytesCommitted,
      recordsRejected,
      wasBackfilled(streamStats),
      wasResumed(streamStats),
    )
  }

  private fun selectResumedStats(streamSyncStats: List<StreamSyncStats>): List<StreamSyncStats> {
    var i = streamSyncStats.size - 1
    while (i > 0 && java.lang.Boolean.TRUE === streamSyncStats[i].wasResumed) {
      i--
    }
    return streamSyncStats.subList(i, streamSyncStats.size)
  }

  @JvmStatic
  fun wasBackfilled(streamStats: List<StreamSyncStats>): Optional<Boolean> {
    // if a stream is a backfill, at least one attempt will be marked as backfill
    if (streamStats.stream().anyMatch { syncStats: StreamSyncStats -> syncStats.wasBackfilled != null && syncStats.wasBackfilled }) {
      return Optional.of(true)
    }

    // if no attempts were marked as backfill then either the stream is not a backfill or
    // the backfill flag hasn't been set yet (flag is set when the attempt completes)
    if (streamStats.stream().anyMatch { syncStats: StreamSyncStats -> syncStats.wasBackfilled != null && !syncStats.wasBackfilled }) {
      return Optional.of(false)
    }

    return Optional.empty()
  }

  fun wasResumed(streamStats: List<StreamSyncStats>): Optional<Boolean> {
    // if a stream was resumed, at least one attempt will be marked as resumed
    if (streamStats.stream().anyMatch { syncStats: StreamSyncStats -> syncStats.wasResumed != null && syncStats.wasResumed }) {
      return Optional.of(true)
    }

    // if no attempts were marked as resumed then either the stream is not resumed or
    // the resumed flag hasn't been set yet (flag is set when the attempt completes)
    if (streamStats.stream().anyMatch { syncStats: StreamSyncStats -> syncStats.wasResumed != null && !syncStats.wasResumed }) {
      return Optional.of(false)
    }

    return Optional.empty()
  }

  @JvmStatic
  @Throws(IOException::class)
  fun hydrateWithStats(
    jobReads: List<JobWithAttemptsRead>,
    jobs: List<Job>,
    hydrateAggregatedStats: Boolean,
    jobPersistence: JobPersistence,
  ) {
    val jobIds = jobReads.stream().map { r: JobWithAttemptsRead -> r.job.id }.toList()
    val attemptStats: Map<JobAttemptPair, JobPersistence.AttemptStats> = jobPersistence.getAttemptStats(jobIds)

    log.info("Attempt stats: {}", attemptStats)
    val jobToStreamStats: MutableMap<Long, Map<StreamNameAndNamespace, MutableList<StreamSyncStats>>> = HashMap()
    for (jwar in jobReads) {
      val streamAttemptStats: MutableMap<StreamNameAndNamespace, MutableList<StreamSyncStats>> = HashMap()
      jobToStreamStats.putIfAbsent(jwar.job.id, streamAttemptStats)
      log.info("Hydrating job {}", jwar.job.id)
      for (attempt in jwar.attempts) {
        val stat = attemptStats[JobAttemptPair(jwar.job.id, attempt.id.toInt())]
        if (stat == null) {
          log.warn("Missing stats for job {} attempt {}", jwar.job.id, attempt.id.toInt())
          continue
        }

        hydrateWithStats(attempt, stat)
        if (hydrateAggregatedStats) {
          stat.perStreamStats.forEach(
            Consumer { s: StreamSyncStats ->
              val streamNameAndNamespace = StreamNameAndNamespace(s.streamName, s.streamNamespace)
              streamAttemptStats.putIfAbsent(streamNameAndNamespace, ArrayList())
              streamAttemptStats[streamNameAndNamespace]!!.add(s)
            },
          )
        }
      }
    }

    if (hydrateAggregatedStats) {
      val jobToStreamSyncMode =
        jobs
          .stream()
          .collect(
            Collectors.toMap(
              Function { j: Job -> j.id },
              Function { job: Job -> JobHistoryHandler.getStreamsToSyncMode(job) },
            ),
          )

      log.debug("Job to stream sync mode: {}", jobToStreamSyncMode)
      jobReads.forEach(
        Consumer { job: JobWithAttemptsRead ->
          val streamToAttemptStats =
            jobToStreamStats[job.job.id]!!
          val streamToSyncMode = jobToStreamSyncMode[job.job.id]!!
          hydrateWithAggregatedStats(job, streamToAttemptStats, streamToSyncMode)
        },
      )
    }
  }

  /**
   * Retrieve stats for a given job id and attempt number and hydrate the api model with the retrieved
   * information.
   *
   * @param a the attempt to hydrate stats for.
   */
  @JvmStatic
  fun hydrateWithStats(
    a: AttemptRead,
    attemptStats: JobPersistence.AttemptStats,
  ) {
    a.totalStats = AttemptStats()

    val combinedStats =
      attemptStats.combinedStats
        ?: // If overall stats are missing, assume stream stats are also missing, since overall stats are
        // easier to produce than stream stats. Exit early.
        return

    a.totalStats
      .estimatedBytes(combinedStats.estimatedBytes)
      .estimatedRecords(combinedStats.estimatedRecords)
      .bytesEmitted(combinedStats.bytesEmitted)
      .recordsEmitted(combinedStats.recordsEmitted)
      .recordsCommitted(combinedStats.recordsCommitted)
      .recordsRejected(combinedStats.recordsRejected)

    val streamStats =
      attemptStats.perStreamStats
        .stream()
        .map { s: StreamSyncStats ->
          AttemptStreamStats()
            .streamName(s.streamName)
            .streamNamespace(s.streamNamespace)
            .stats(
              AttemptStats()
                .bytesEmitted(s.stats.bytesEmitted)
                .recordsEmitted(s.stats.recordsEmitted)
                .recordsCommitted(s.stats.recordsCommitted)
                .recordsRejected(s.stats.recordsRejected)
                .estimatedBytes(s.stats.estimatedBytes)
                .estimatedRecords(s.stats.estimatedRecords),
            )
        }.collect(Collectors.toList())
    a.streamStats = streamStats
  }

  // WARNING!!!!! These stats are used for billing, be careful when changing this logic.
  private fun hydrateWithAggregatedStats(
    job: JobWithAttemptsRead,
    streamToAttemptStats: Map<StreamNameAndNamespace, MutableList<StreamSyncStats>>,
    streamToSyncMode: Map<StreamNameAndNamespace, SyncMode>,
  ) {
    val streamAggregatedStats: MutableList<StreamStatsRecord> = ArrayList()
    streamToSyncMode.keys.forEach(
      Consumer<StreamNameAndNamespace> { streamNameAndNamespace: StreamNameAndNamespace ->
        if (!streamToAttemptStats.containsKey(streamNameAndNamespace)) {
          log.debug("No stats have been persisted for job {} stream {}.", job.job.id, streamNameAndNamespace)
          return@Consumer
        }
        val streamStats: List<StreamSyncStats> = streamToAttemptStats[streamNameAndNamespace]!!
        val syncMode = streamToSyncMode[streamNameAndNamespace]

        val aggregatedStats = getAggregatedStats(syncMode!!, streamStats)
        streamAggregatedStats.add(aggregatedStats)
      },
    )

    val jobAggregatedStats = getJobAggregatedStats(streamAggregatedStats)
    job.job.aggregatedStats = jobAggregatedStats
    job.job.streamAggregatedStats =
      streamAggregatedStats
        .stream()
        .map { s: StreamStatsRecord ->
          StreamStats()
            .streamName(s.streamName)
            .streamNamespace(s.streamNamespace)
            .recordsEmitted(s.recordsEmitted)
            .bytesEmitted(s.bytesEmitted)
            .recordsCommitted(s.recordsCommitted)
            .bytesCommitted(s.bytesCommitted)
            .recordsRejected(s.recordsRejected)
            .wasBackfilled(s.wasBackfilled.orElse(null))
            .wasResumed(s.wasResumed.orElse(null))
        }.collect(Collectors.toList<@Valid StreamStats?>())
  }

  private fun getJobAggregatedStats(streamStats: List<StreamStatsRecord>): JobAggregatedStats =
    JobAggregatedStats()
      .recordsEmitted(streamStats.stream().mapToLong(StreamStatsRecord::recordsEmitted).sum())
      .bytesEmitted(streamStats.stream().mapToLong(StreamStatsRecord::bytesEmitted).sum())
      .recordsCommitted(streamStats.stream().mapToLong(StreamStatsRecord::recordsCommitted).sum())
      .bytesCommitted(streamStats.stream().mapToLong(StreamStatsRecord::bytesCommitted).sum())
      .recordsRejected(streamStats.stream().mapToLong(StreamStatsRecord::recordsRejected).sum())

  @JvmStatic
  fun getJobIdToJobWithAttemptsReadMap(
    jobs: List<Job>,
    jobPersistence: JobPersistence,
  ): Map<Long, JobWithAttemptsRead> {
    if (jobs.isEmpty()) {
      return emptyMap()
    }
    try {
      val jobReads = jobs.stream().map { obj: Job -> JobConverter.getJobWithAttemptsRead(obj) }.collect(Collectors.toList())

      hydrateWithStats(jobReads, jobs, true, jobPersistence)
      return jobReads.stream().collect(
        Collectors.toMap(
          Function { j: JobWithAttemptsRead -> j.job.id },
          Function { j: JobWithAttemptsRead -> j },
        ),
      )
    } catch (e: IOException) {
      throw RuntimeException(e)
    }
  }

  @JvmRecord
  data class StreamStatsRecord(
    val streamName: String,
    val streamNamespace: String?,
    @JvmField val recordsEmitted: Long,
    @JvmField val bytesEmitted: Long,
    @JvmField val recordsCommitted: Long,
    @JvmField val bytesCommitted: Long,
    @JvmField val recordsRejected: Long,
    val wasBackfilled: Optional<Boolean>,
    val wasResumed: Optional<Boolean>,
  )
}
