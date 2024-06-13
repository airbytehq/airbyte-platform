package io.airbyte.workers.internal.syncpersistence

import datadog.trace.api.Trace
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.AttemptStats
import io.airbyte.api.client.model.generated.AttemptStreamStats
import io.airbyte.api.client.model.generated.ConnectionState
import io.airbyte.api.client.model.generated.ConnectionStateCreateOrUpdate
import io.airbyte.api.client.model.generated.SaveStatsRequestBody
import io.airbyte.commons.converters.StateConverter
import io.airbyte.config.SyncStats
import io.airbyte.config.helpers.StateMessageHelper
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.MetricClientFactory
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.metrics.lib.OssMetricsRegistry
import io.airbyte.protocol.models.AirbyteEstimateTraceMessage
import io.airbyte.protocol.models.AirbyteRecordMessage
import io.airbyte.protocol.models.AirbyteStateMessage
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog
import io.airbyte.workers.internal.bookkeeping.SyncStatsTracker
import io.airbyte.workers.internal.bookkeeping.getPerStreamStats
import io.airbyte.workers.internal.bookkeeping.getTotalStats
import io.airbyte.workers.internal.stateaggregator.StateAggregator
import io.airbyte.workers.internal.stateaggregator.StateAggregatorFactory
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Parameter
import io.micronaut.context.annotation.Prototype
import io.micronaut.context.annotation.Value
import io.micronaut.core.annotation.Creator
import jakarta.inject.Named
import java.util.UUID
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import kotlin.jvm.optionals.getOrNull

interface SyncPersistence : SyncStatsTracker, AutoCloseable {
  /**
   * Persist a state for a given connectionId.
   *
   * @param connectionId the connection
   * @param stateMessage stateMessage to persist
   */
  fun persist(
    connectionId: UUID,
    stateMessage: AirbyteStateMessage,
  )
}

private val logger = KotlinLogging.logger {}
private const val RUN_IMMEDIATELY: Long = 0
private const val FLUSH_TERMINATION_TIMEOUT_IN_SECONDS: Long = 60

// For overriding the jitter config when testing
data class RetryWithJitterConfig(val jitterMaxIntervalSecs: Int, val finalIntervalSecs: Int, val maxTries: Int)

@Prototype
class SyncPersistenceImpl
  @Creator
  constructor(
    private val airbyteApiClient: AirbyteApiClient,
    private val stateAggregatorFactory: StateAggregatorFactory,
    @Named("syncPersistenceExecutorService") private val stateFlushExecutorService: ScheduledExecutorService,
    @Value("\${airbyte.worker.replication.persistence-flush-period-sec}") private val stateFlushPeriodInSeconds: Long,
    private val metricClient: MetricClient,
    @param:Parameter private val syncStatsTracker: SyncStatsTracker,
    @param:Parameter private val connectionId: UUID,
    @param:Parameter private val workspaceId: UUID,
    @param:Parameter private val jobId: Long,
    @param:Parameter private val attemptNumber: Int,
    @param:Parameter private val catalog: ConfiguredAirbyteCatalog,
  ) : SyncPersistence, SyncStatsTracker by syncStatsTracker {
    private var stateBuffer = stateAggregatorFactory.create()
    private var stateFlushFuture: ScheduledFuture<*>? = null
    private var isReceivingStats = false
    private var stateToFlush: StateAggregator? = null
    private var statsToPersist: SaveStatsRequestBody? = null
    private var retryWithJitterConfig: RetryWithJitterConfig? = null

    protected constructor(
      airbyteApiClient: AirbyteApiClient,
      stateAggregatorFactory: StateAggregatorFactory,
      syncStatsTracker: SyncStatsTracker,
      scheduledExecutorService: ScheduledExecutorService,
      stateFlushPeriodInSeconds: Long,
      retryWithJitterConfig: RetryWithJitterConfig?,
      connectionId: UUID,
      workspaceId: UUID,
      jobId: Long,
      attemptNumber: Int,
      catalog: ConfiguredAirbyteCatalog,
    ) : this(
      airbyteApiClient = airbyteApiClient,
      stateAggregatorFactory = stateAggregatorFactory,
      stateFlushExecutorService = scheduledExecutorService,
      stateFlushPeriodInSeconds = stateFlushPeriodInSeconds,
      syncStatsTracker = syncStatsTracker,
      metricClient = MetricClientFactory.getMetricClient(),
      connectionId = connectionId,
      workspaceId = workspaceId,
      jobId = jobId,
      attemptNumber = attemptNumber,
      catalog = catalog,
    ) {
      this.retryWithJitterConfig = retryWithJitterConfig
    }

    @Trace
    override fun persist(
      connectionId: UUID,
      stateMessage: AirbyteStateMessage,
    ) {
      require(this.connectionId == connectionId) {
        "Invalid connectionId $connectionId, expected ${this.connectionId}"
      }

      metricClient.count(OssMetricsRegistry.STATE_BUFFERING, 1)
      stateBuffer.ingest(stateMessage)
      startBackgroundFlushStateTask(connectionId)
    }

    private fun startBackgroundFlushStateTask(connectionId: UUID) {
      if (stateFlushFuture != null) {
        return
      }

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
     * If there is already flush in progress, wait for it to terminate. If it didn't terminate during
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

      // Wait for previous running task to terminate
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
      metricClient.count(OssMetricsRegistry.STATE_COMMIT_CLOSE_SUCCESSFUL, 1)

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
      metricClient.count(OssMetricsRegistry.STATS_COMMIT_CLOSE_SUCCESSFUL, 1)
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
      stateBuffer = stateAggregatorFactory.create()
      if (stateToFlush == null) {
        // Happy path, previous flush was successful
        stateToFlush = stateBufferToFlush
      } else {
        // Merging states from the previous attempt with the incoming buffer to flush
        // ? is required here as [stateToFlush] is mutable and there is no guarantee that iw hasn't changed since the previous
        // null check
        stateToFlush?.ingest(stateBufferToFlush)
      }

      // We prepare stats to commit. We generate the payload here to keep track as close as possible to
      // the states that are going to be persisted.
      // We also only want to generate the stats payload when roll-over state buffers. This is to avoid
      // updating the committed data counters ahead of the states because this counter is currently
      // decoupled from the state persistence.
      // This design favoring accuracy of committed data counters over freshness of emitted data counters.
      if (isReceivingStats && stateToFlush?.isEmpty() == false) {
        // TODO figure out a way to remove the double-bangs
        statsToPersist = buildSaveStatsRequest(syncStatsTracker, jobId, attemptNumber, connectionId)
      }
    }

    private fun doFlushState() {
      if (stateToFlush == null || stateToFlush?.isEmpty() == true) {
        return
      }

      val state = stateToFlush?.getAggregated() ?: return
      val maybeStateWrapper = StateMessageHelper.getTypedState(state.state).getOrNull() ?: return

      metricClient.count(OssMetricsRegistry.STATE_COMMIT_ATTEMPT, 1)

      val stateApiRequest =
        ConnectionStateCreateOrUpdate(connectionId = connectionId, connectionState = StateConverter.toClient(connectionId, maybeStateWrapper))

      try {
        airbyteApiClient.stateApi.createOrUpdateState(stateApiRequest)
      } catch (e: Exception) {
        metricClient.count(OssMetricsRegistry.STATE_COMMIT_ATTEMPT_FAILED, 1)
        throw e
      }

      // Only reset stateToFlush if the API call was successful
      stateToFlush = null
      metricClient.count(OssMetricsRegistry.STATE_COMMIT_ATTEMPT_SUCCESSFUL, 1)
    }

    private fun doFlushStats() {
      if (!hasStatsToFlush()) {
        return
      }

      metricClient.count(OssMetricsRegistry.STATS_COMMIT_ATTEMPT, 1)
      try {
        airbyteApiClient.attemptApi.saveStats(statsToPersist!!)
      } catch (e: Exception) {
        metricClient.count(OssMetricsRegistry.STATS_COMMIT_ATTEMPT_FAILED, 1)
        throw e
      }

      statsToPersist = null
      metricClient.count(OssMetricsRegistry.STATS_COMMIT_ATTEMPT_SUCCESSFUL, 1)
    }

    private fun hasStatesToFlush(): Boolean = !stateBuffer.isEmpty() || stateToFlush != null

    private fun hasStatsToFlush(): Boolean = isReceivingStats && statsToPersist != null

    override fun updateStats(recordMessage: AirbyteRecordMessage) {
      isReceivingStats = true
      syncStatsTracker.updateStats(recordMessage)
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
  }

private fun isStateEmpty(connectionState: ConnectionState?) = connectionState?.state?.isEmpty ?: false

private fun buildSaveStatsRequest(
  syncStatsTracker: SyncStatsTracker,
  jobId: Long,
  attemptNumber: Int,
  connectionId: UUID,
): SaveStatsRequestBody {
  val totalSyncStats = syncStatsTracker.getTotalStats(false)
  val streamSyncStats = syncStatsTracker.getPerStreamStats(false)

  return SaveStatsRequestBody(
    jobId = jobId,
    attemptNumber = attemptNumber,
    stats = totalSyncStats.toAttemptStats(),
    connectionId = connectionId,
    streamStats =
      streamSyncStats.map {
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
  )

private fun MetricClient.emitFailedStateCloseMetrics(connectionId: UUID?) {
  val attribute: MetricAttribute? = connectionId?.let { MetricAttribute(MetricTags.CONNECTION_ID, it.toString()) }
  count(OssMetricsRegistry.STATE_COMMIT_NOT_ATTEMPTED, 1, attribute)
}

private fun MetricClient.emitFailedStatsCloseMetrics(connectionId: UUID?) {
  val attribute: MetricAttribute? = connectionId?.let { MetricAttribute(MetricTags.CONNECTION_ID, it.toString()) }
  count(OssMetricsRegistry.STATS_COMMIT_NOT_ATTEMPTED, 1, attribute)
}
