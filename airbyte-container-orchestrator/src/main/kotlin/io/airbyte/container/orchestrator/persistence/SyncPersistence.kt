/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.persistence

import datadog.trace.api.Trace
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.AttemptStats
import io.airbyte.api.client.model.generated.AttemptStreamStats
import io.airbyte.api.client.model.generated.ConnectionStateCreateOrUpdate
import io.airbyte.api.client.model.generated.SaveStatsRequestBody
import io.airbyte.commons.converters.StateConverter
import io.airbyte.config.SyncStats
import io.airbyte.config.helpers.StateMessageHelper
import io.airbyte.container.orchestrator.bookkeeping.SyncStatsTracker
import io.airbyte.container.orchestrator.bookkeeping.getPerStreamStats
import io.airbyte.container.orchestrator.bookkeeping.getTotalStats
import io.airbyte.container.orchestrator.bookkeeping.state.StateAggregator
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.protocol.models.v0.AirbyteEstimateTraceMessage
import io.airbyte.protocol.models.v0.AirbyteRecordMessage
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.UUID
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import kotlin.jvm.optionals.getOrNull

interface SyncPersistence :
  SyncStatsTracker,
  AutoCloseable {
  /**
   * Buffers a state for a given connectionId for eventual persistence.
   *
   * @param connectionId the connection
   * @param stateMessage stateMessage to persist
   */
  fun accept(
    connectionId: UUID,
    stateMessage: AirbyteStateMessage,
  )
}

private val logger = KotlinLogging.logger {}
private const val RUN_IMMEDIATELY: Long = 0
private const val FLUSH_TERMINATION_TIMEOUT_IN_SECONDS: Long = 60

@Singleton
@Named("syncPersistence")
class SyncPersistenceImpl(
  private val airbyteApiClient: AirbyteApiClient,
  @Named("stateAggregator") private val stateBuffer: StateAggregator,
  @Named("syncPersistenceExecutorService") private val stateFlushExecutorService: ScheduledExecutorService,
  @Value("\${airbyte.worker.replication.persistence-flush-period-sec}") private val stateFlushPeriodInSeconds: Long,
  private val metricClient: MetricClient,
  @Named("parallelStreamStatsTracker") private val syncStatsTracker: SyncStatsTracker,
  @Named("connectionId") private val connectionId: UUID,
  @Value("\${airbyte.job-id}") private val jobId: Long,
  @Named("attemptId") private val attemptNumber: Int,
) : SyncPersistence,
  SyncStatsTracker by syncStatsTracker {
  private var stateFlushFuture: ScheduledFuture<*>? = null
  private var isReceivingStats = false
  private var stateToFlush: StateAggregator? = null
  private var persistedStats: SaveStatsRequestBody? = null
  private var statsToPersist: SaveStatsRequestBody? = null

  init {
    startBackgroundFlushStateTask(connectionId)
  }

  @Trace
  override fun accept(
    connectionId: UUID,
    stateMessage: AirbyteStateMessage,
  ) {
    require(this.connectionId == connectionId) {
      "Invalid connectionId $connectionId, expected ${this.connectionId}"
    }

    metricClient.count(metric = OssMetricsRegistry.STATE_BUFFERING)
    stateBuffer.ingest(stateMessage)
  }

  private fun startBackgroundFlushStateTask(connectionId: UUID) {
    // Making sure we only start one of background flush task
    synchronized(this) {
      if (stateFlushFuture == null) {
        logger.info { "starting state flush thread for connectionId $connectionId" }
        stateFlushFuture =
          stateFlushExecutorService.scheduleAtFixedRate(
            { this.flush() },
            RUN_IMMEDIATELY,
            stateFlushPeriodInSeconds,
            TimeUnit.SECONDS,
          )
      }
    }
  }

  /**
   * Stop background data flush thread and attempt to flush pending data
   *
   *
   * If there is already a flush in progress, wait for it to terminate. If it didn't terminate during
   * the allocated time, we exit rather than attempting a concurrent write that could lead to
   * non-deterministic behavior.
   *
   *
   * For the final flush, we will retry in case of failures since there is no more "scheduled" attempt
   * after this.
   */
  override fun close() {
    // stop the buffered refresh
    stateFlushExecutorService.shutdown()

    // Wait for the previous running task to terminate
    try {
      val terminated = stateFlushExecutorService.awaitTermination(FLUSH_TERMINATION_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS)
      if (!terminated) {
        if (stateToFlush != null && !stateToFlush!!.isEmpty()) {
          metricClient.emitFailedStateCloseMetrics(connectionId)
          metricClient.emitFailedStatsCloseMetrics(connectionId)
        }

        // Ongoing flush failed to terminate within the allocated time
        logger.info { "Pending persist operation took too long to complete, most recent states may have been lost" }

        // This is the hard case, if the backend persisted the data, we may write duplicate
        // We exit to avoid non-deterministic write attempts
        return
      }
    } catch (e: InterruptedException) {
      if (stateToFlush != null && !stateToFlush!!.isEmpty()) {
        metricClient.emitFailedStateCloseMetrics(connectionId)
        metricClient.emitFailedStatsCloseMetrics(connectionId)
      }

      // The current thread is getting interrupted
      logger.info(e) { "SyncPersistence has been interrupted while terminating, most recent states may have been lost" }

      // This is also a hard case, if the backend persisted the data, we may write duplicate
      // We exit to avoid non-deterministic write attempts
      return
    }
    if (hasStatesToFlush()) {
      // we still have data to flush
      prepareDataForFlush()
      try {
        doFlushState()
      } catch (e: Exception) {
        if (stateToFlush?.isEmpty() == false) {
          metricClient.emitFailedStateCloseMetrics(connectionId)
          metricClient.emitFailedStatsCloseMetrics(connectionId)
        }
        throw e
      }
    }

    // At this point, the final state flush is either successful or there was no state left to flush.
    // From a connection point of view, it should be considered a success since no state are lost.
    metricClient.count(metric = OssMetricsRegistry.STATE_COMMIT_CLOSE_SUCCESSFUL)

    // On close, this check is independent of hasDataToFlush. We could be in a state where state flush
    // was successful but stats flush failed, so we should check for stats to flush regardless of the
    // states.
    if (hasStatsToFlush()) {
      try {
        doFlushStats()
      } catch (e: Exception) {
        metricClient.emitFailedStatsCloseMetrics(connectionId)
        throw e
      }
    }
    metricClient.count(metric = OssMetricsRegistry.STATS_COMMIT_CLOSE_SUCCESSFUL)
  }

  /**
   * Flush state and stats for the ScheduledExecutorService
   *
   *
   * This method is swallowing exceptions on purpose. We do not want to fail or retry in a regular
   * run, the retry is deferred to the next run which will merge the data from the previous failed
   * attempt and the recent buffered data.
   */
  private fun flush() {
    prepareDataForFlush()
    try {
      doFlushState()
      try {
        // We only flush stats if there was no state flush errors.
        // Even if there are no states to flush, we should still try to flush stats in case previous stats
        // flush failed
        doFlushStats()
      } catch (e: Exception) {
        logger.warn(e) { "Failed to persist stats for connectionId $connectionId, it will be retried as part of the next flush" }
      }
    } catch (e: Exception) {
      logger.warn(e) { "Failed to persist state for connectionId $connectionId, it will be retried as part of the next flush" }
    }
  }

  private fun prepareDataForFlush() {
    val stateBufferToFlush = stateBuffer
    if (stateToFlush == null) {
      // Happy path, previous flush was successful
      stateToFlush = stateBufferToFlush
    } else {
      // Merging states from the previous attempt with the incoming buffer to flush
      // ? is required here as [stateToFlush] is mutable and there is no guarantee that iw hasn't changed since the previous
      // null check
      stateToFlush?.ingest(stateBufferToFlush)
    }

    if (!isReceivingStats) {
      return
    }

    statsToPersist = buildSaveStatsRequest(syncStatsTracker, jobId, attemptNumber, connectionId)
  }

  private fun doFlushState() {
    if (stateToFlush == null || stateToFlush?.isEmpty() == true) {
      return
    }

    val state = stateToFlush?.getAggregated() ?: return
    val maybeStateWrapper = StateMessageHelper.getTypedState(state.state).getOrNull() ?: return

    metricClient.count(metric = OssMetricsRegistry.STATE_COMMIT_ATTEMPT)

    val stateApiRequest =
      ConnectionStateCreateOrUpdate(connectionId = connectionId, connectionState = StateConverter.toClient(connectionId, maybeStateWrapper))

    try {
      airbyteApiClient.stateApi.createOrUpdateState(stateApiRequest)
    } catch (e: Exception) {
      metricClient.count(metric = OssMetricsRegistry.STATE_COMMIT_ATTEMPT_FAILED)
      throw e
    }

    // Only clear and reset stateToFlush if the API call was successful
    stateToFlush?.clear()
    stateToFlush = null
    metricClient.count(metric = OssMetricsRegistry.STATE_COMMIT_ATTEMPT_SUCCESSFUL)
  }

  private fun doFlushStats() {
    if (!hasStatsToFlush()) {
      return
    }

    metricClient.count(metric = OssMetricsRegistry.STATS_COMMIT_ATTEMPT)
    try {
      airbyteApiClient.attemptApi.saveStats(statsToPersist!!)
    } catch (e: Exception) {
      metricClient.count(metric = OssMetricsRegistry.STATS_COMMIT_ATTEMPT_FAILED)
      throw e
    }

    persistedStats = statsToPersist
    statsToPersist = null
    metricClient.count(metric = OssMetricsRegistry.STATS_COMMIT_ATTEMPT_SUCCESSFUL)
  }

  private fun hasStatesToFlush(): Boolean = !stateBuffer.isEmpty() || stateToFlush != null

  private fun hasStatsToFlush(): Boolean = isReceivingStats && statsToPersist != null && statsToPersist != persistedStats

  override fun updateStats(recordMessage: AirbyteRecordMessage) {
    isReceivingStats = true
    syncStatsTracker.updateStats(recordMessage)
  }

  override fun updateStatsFromDestination(recordMessage: AirbyteRecordMessage) {
    isReceivingStats = true
    syncStatsTracker.updateStatsFromDestination(recordMessage)
  }

  override fun updateEstimates(estimate: AirbyteEstimateTraceMessage) {
    isReceivingStats = true
    syncStatsTracker.updateEstimates(estimate)
  }

  override fun updateSourceStatesStats(stateMessage: AirbyteStateMessage) {
    isReceivingStats = true
    syncStatsTracker.updateSourceStatesStats(stateMessage)
  }

  override fun updateDestinationStateStats(stateMessage: AirbyteStateMessage) {
    isReceivingStats = true
    syncStatsTracker.updateDestinationStateStats(stateMessage)
  }

  override fun endOfReplication(completedSuccessfully: Boolean) {
    syncStatsTracker.endOfReplication(completedSuccessfully)
  }
}

private fun buildSaveStatsRequest(
  syncStatsTracker: SyncStatsTracker,
  jobId: Long,
  attemptNumber: Int,
  connectionId: UUID,
): SaveStatsRequestBody {
  val streamSyncStats = syncStatsTracker.getPerStreamStats(false)
  val totalSyncStats = syncStatsTracker.getTotalStats(streamSyncStats, false)

  return SaveStatsRequestBody(
    jobId = jobId,
    attemptNumber = attemptNumber,
    stats = totalSyncStats.toAttemptStats(),
    connectionId = connectionId,
    streamStats =
      streamSyncStats
        .map {
          AttemptStreamStats(streamName = it.streamName, streamNamespace = it.streamNamespace, stats = it.stats.toAttemptStats())
        }.toList(),
  )
}

private fun SyncStats.toAttemptStats(): AttemptStats =
  AttemptStats(
    recordsEmitted = recordsEmitted,
    bytesEmitted = bytesEmitted,
    estimatedBytes = estimatedBytes,
    estimatedRecords = estimatedRecords,
    bytesCommitted = bytesCommitted,
    recordsCommitted = recordsCommitted,
    recordsRejected = recordsRejected,
  )

private fun MetricClient.emitFailedStateCloseMetrics(connectionId: UUID?) {
  val attribute: MetricAttribute? = connectionId?.let { MetricAttribute(MetricTags.CONNECTION_ID, it.toString()) }
  count(metric = OssMetricsRegistry.STATE_COMMIT_NOT_ATTEMPTED, attributes = arrayOf(attribute))
}

private fun MetricClient.emitFailedStatsCloseMetrics(connectionId: UUID?) {
  val attribute: MetricAttribute? = connectionId?.let { MetricAttribute(MetricTags.CONNECTION_ID, it.toString()) }
  count(metric = OssMetricsRegistry.STATS_COMMIT_NOT_ATTEMPTED, attributes = arrayOf(attribute))
}
