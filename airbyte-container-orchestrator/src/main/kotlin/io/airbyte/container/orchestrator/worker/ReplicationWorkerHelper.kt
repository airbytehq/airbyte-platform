/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.annotations.VisibleForTesting
import io.airbyte.commons.io.LineGobbler
import io.airbyte.config.FailureReason
import io.airbyte.config.MapperConfig
import io.airbyte.config.PerformanceMetrics
import io.airbyte.config.ReplicationAttemptSummary
import io.airbyte.config.ReplicationOutput
import io.airbyte.config.StandardSyncSummary.ReplicationStatus
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.SyncStats
import io.airbyte.config.WorkerDestinationConfig
import io.airbyte.container.orchestrator.bookkeeping.AirbyteMessageOrigin
import io.airbyte.container.orchestrator.bookkeeping.AirbyteMessageTracker
import io.airbyte.container.orchestrator.bookkeeping.SyncStatsTracker
import io.airbyte.container.orchestrator.bookkeeping.events.ReplicationAirbyteMessageEvent
import io.airbyte.container.orchestrator.bookkeeping.events.ReplicationAirbyteMessageEventPublishingHelper
import io.airbyte.container.orchestrator.bookkeeping.getPerStreamStats
import io.airbyte.container.orchestrator.bookkeeping.getTotalStats
import io.airbyte.container.orchestrator.bookkeeping.streamstatus.StreamStatusTracker
import io.airbyte.container.orchestrator.tracker.AnalyticsMessageTracker
import io.airbyte.container.orchestrator.tracker.StreamStatusCompletionTracker
import io.airbyte.container.orchestrator.tracker.ThreadedTimeTracker
import io.airbyte.container.orchestrator.worker.context.ReplicationContext
import io.airbyte.container.orchestrator.worker.filter.FieldSelector
import io.airbyte.container.orchestrator.worker.io.AirbyteDestination
import io.airbyte.container.orchestrator.worker.io.AirbyteSource
import io.airbyte.container.orchestrator.worker.model.adapter.AirbyteJsonRecordAdapter
import io.airbyte.container.orchestrator.worker.model.attachIdToStateMessageFromSource
import io.airbyte.container.orchestrator.worker.util.BytesSizeHelper.byteCountToDisplaySize
import io.airbyte.mappers.application.RecordMapper
import io.airbyte.mappers.transformations.DestinationCatalogGenerator
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteMessage.Type.RECORD
import io.airbyte.protocol.models.v0.AirbyteMessage.Type.STATE
import io.airbyte.protocol.models.v0.AirbyteTraceMessage
import io.airbyte.workers.WorkerUtils
import io.airbyte.workers.exception.WorkerException
import io.airbyte.workers.helper.ResumableFullRefreshStatsHelper
import io.airbyte.workers.internal.AirbyteMapper
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.nio.file.Path
import java.util.Optional

private val logger = KotlinLogging.logger {}

@Singleton
class ReplicationWorkerHelper(
  private val fieldSelector: FieldSelector?,
  private val mapper: AirbyteMapper?,
  private val messageTracker: AirbyteMessageTracker,
  private val eventPublisher: ReplicationAirbyteMessageEventPublishingHelper,
  private val timeTracker: ThreadedTimeTracker,
  private val analyticsTracker: AnalyticsMessageTracker,
  private val streamStatusCompletionTracker: StreamStatusCompletionTracker,
  @Named("parallelStreamStatsTracker") val syncStatsTracker: SyncStatsTracker,
  private val streamStatusTracker: StreamStatusTracker,
  private val recordMapper: RecordMapper,
  private val replicationWorkerState: ReplicationWorkerState,
  private val context: ReplicationContextProvider.Context,
  destinationCatalogGenerator: DestinationCatalogGenerator,
  private val metricClient: MetricClient,
) {
  private val streamMappers: Map<StreamDescriptor, List<MapperConfig>>
  private val destinationConfig: WorkerDestinationConfig
  private val mappersConfigured: Boolean
  private val metricAttrs: MutableList<MetricAttribute> = mutableListOf()

  private fun toConnectionAttrs(ctx: ReplicationContext?): List<MetricAttribute> {
    if (ctx == null) {
      return listOf()
    }

    return buildList {
      add(MetricAttribute(MetricTags.CONNECTION_ID, ctx.connectionId.toString()))
      add(MetricAttribute(MetricTags.JOB_ID, ctx.jobId.toString()))
      add(MetricAttribute(MetricTags.ATTEMPT_NUMBER, ctx.attempt.toString()))
      add(MetricAttribute(MetricTags.IS_RESET, ctx.isReset.toString()))
    }
  }

  init {
    with(metricAttrs) {
      clear()
      addAll(toConnectionAttrs(context.replicationContext))
    }

    analyticsTracker.ctx = context.replicationContext

    if (context.supportRefreshes) {
      // if configured airbyte catalog has full refresh with state
      val resumedFRStreams = ResumableFullRefreshStatsHelper().getResumedFullRefreshStreams(context.configuredCatalog, context.replicationInput.state)
      logger.info { "Number of Resumed Full Refresh Streams: {${resumedFRStreams.size}}" }
      if (resumedFRStreams.isNotEmpty()) {
        resumedFRStreams.forEach { streamDescriptor ->
          logger.info { " Resumed stream name: ${streamDescriptor.name} namespace: ${streamDescriptor.namespace}" }
        }
      }
    }

    streamStatusCompletionTracker.startTracking(context.configuredCatalog, context.supportRefreshes)

    if (context.configuredCatalog.streams.isEmpty()) {
      metricClient.count(metric = OssMetricsRegistry.SYNC_WITH_EMPTY_CATALOG, attributes = metricAttrs.toTypedArray())
    }

    val catalogWithoutInvalidMappers = destinationCatalogGenerator.generateDestinationCatalog(context.configuredCatalog)
    streamMappers = catalogWithoutInvalidMappers.catalog.streams.associate { stream -> stream.streamDescriptor to stream.mappers }
    destinationConfig =
      WorkerUtils.syncToWorkerDestinationConfig(context.replicationInput).apply {
        catalog = mapper?.mapCatalog(catalog) ?: catalog
        supportRefreshes = context.supportRefreshes
      }
    mappersConfigured = streamMappers.isNotEmpty()
    timeTracker.trackReplicationStartTime()
  }

  private var recordsRead: Long = 0
  private var totalRecordsRead: Long = 0

  fun initialize(jobRoot: Path) {
    ApmTraceUtils.addTagsToTrace(
      context.replicationContext.connectionId,
      context.replicationContext.attempt.toLong(),
      context.replicationContext.jobId.toString(),
      jobRoot,
    )
  }

  fun startDestination(
    destination: AirbyteDestination,
    jobRoot: Path,
  ) {
    timeTracker.trackDestinationWriteStartTime()
    try {
      destination.start(destinationConfig, jobRoot)
    } catch (e: Exception) {
      throw WorkerException("Unable to start destination", e)
    }
  }

  fun startSource(
    source: AirbyteSource,
    replicationInput: ReplicationInput,
    jobRoot: Path,
  ) {
    timeTracker.trackSourceReadStartTime()
    val sourceConfig =
      WorkerUtils.syncToWorkerSourceConfig(replicationInput).also {
        fieldSelector?.populateFields(it.catalog)
      }
    try {
      source.start(sourceConfig, jobRoot, context.replicationContext.connectionId)
    } catch (e: Exception) {
      throw WorkerException("Unable to start source", e)
    }
  }

  fun endOfReplication() {
    timeTracker.trackReplicationEndTime()
    messageTracker.endOfReplication(
      completedSuccessfully =
        !replicationWorkerState.cancelled &&
          !replicationWorkerState.hasFailed &&
          !replicationWorkerState.shouldAbort,
    )
    analyticsTracker.flush()
  }

  fun endOfSource() {
    val bytes = byteCountToDisplaySize(syncStatsTracker.getTotalBytesEmitted())
    logger.info { "Total records read: ($bytes)" }
    fieldSelector?.reportMetrics(context.replicationContext.sourceId)
    timeTracker.trackSourceReadEndTime()
  }

  fun endOfDestination() {
    timeTracker.trackDestinationWriteEndTime()
  }

  fun processMessageFromDestination(destinationRawMessage: AirbyteMessage) {
    val message = mapper?.revertMap(destinationRawMessage) ?: destinationRawMessage
    internalProcessMessageFromDestination(message)
  }

  fun getStreamStatusToSend(exitValue: Int): List<AirbyteMessage> = streamStatusCompletionTracker.finalize(exitValue, mapper)

  @JvmOverloads
  @Throws(JsonProcessingException::class)
  fun getReplicationOutput(performanceMetrics: PerformanceMetrics? = null): ReplicationOutput {
    val outputStatus =
      when {
        replicationWorkerState.cancelled -> ReplicationStatus.CANCELLED
        replicationWorkerState.hasFailed -> ReplicationStatus.FAILED
        else -> ReplicationStatus.COMPLETED
      }
    val completed = outputStatus == ReplicationStatus.COMPLETED
    val totalStats = getTotalStats(timeTracker, completed)
    val streamStats = syncStatsTracker.getPerStreamStats(completed)

    if (!completed && syncStatsTracker.getUnreliableStateTimingMetrics()) {
      logger.warn { "Unreliable committed record counts; setting committed record stats to null" }
    }

    val summary =
      ReplicationAttemptSummary()
        .withStatus(outputStatus)
        .withRecordsSynced(syncStatsTracker.getTotalRecordsCommitted())
        .withBytesSynced(syncStatsTracker.getTotalBytesCommitted())
        .withTotalStats(totalStats)
        .withStreamStats(streamStats)
        .withStartTime(timeTracker.replicationStartTime)
        .withEndTime(System.currentTimeMillis())
        .withPerformanceMetrics(buildPerformanceMetrics(performanceMetrics))

    val output =
      ReplicationOutput()
        .withReplicationAttemptSummary(summary)
        .withOutputCatalog(destinationConfig.catalog)

    val failures = getFailureReasons(replicationWorkerState.getFailures(), output)
    ObjectMapper().also { om ->
      logger.info { "Sync summary: ${om.writerWithDefaultPrettyPrinter().writeValueAsString(summary)}" }
      logger.info { "Failures: ${om.writerWithDefaultPrettyPrinter().writeValueAsString(failures)}" }
    }

    // Metric to help investigating https://github.com/airbytehq/airbyte/issues/34567
    if (failures.any { f ->
        f.failureOrigin.equals(FailureReason.FailureOrigin.DESTINATION) &&
          f.internalMessage != null &&
          f.internalMessage.contains("Unable to deserialize PartialAirbyteMessage")
      }
    ) {
      metricClient.count(metric = OssMetricsRegistry.DESTINATION_DESERIALIZATION_ERROR, attributes = metricAttrs.toTypedArray())
    }

    LineGobbler.endSection("REPLICATION")
    return output
  }

  @VisibleForTesting
  fun internalProcessMessageFromSource(sourceRawMessage: AirbyteMessage): AirbyteMessage? {
    updateRecordsCount()
    fieldSelector?.filterSelectedFields(sourceRawMessage)
    fieldSelector?.validateSchema(sourceRawMessage)
    messageTracker.acceptFromSource(sourceRawMessage)
    streamStatusTracker.track(sourceRawMessage)
    return when (sourceRawMessage.type) {
      RECORD -> processRecordMessage(sourceRawMessage)
      STATE -> {
        metricClient.count(
          metric = OssMetricsRegistry.STATE_PROCESSED_FROM_SOURCE,
          attributes = metricAttrs.toTypedArray(),
        )
        sourceRawMessage
      }
      else -> {
        if (isAnalyticsMessage(sourceRawMessage)) {
          analyticsTracker.addMessage(sourceRawMessage, AirbyteMessageOrigin.SOURCE)
        } else {
          handleControlMessage(sourceRawMessage, AirbyteMessageOrigin.SOURCE)
        }
        sourceRawMessage
      }
    }
  }

  private fun processRecordMessage(sourceRawMessage: AirbyteMessage): AirbyteMessage? {
    if (mappersConfigured) {
      val adapter = AirbyteJsonRecordAdapter(sourceRawMessage)
      applyTransformationMappers(adapter)
      return if (!adapter.shouldInclude()) {
        syncStatsTracker.updateFilteredOutRecordsStats(sourceRawMessage.record)
        null
      } else {
        sourceRawMessage
      }
    }
    return sourceRawMessage
  }

  private fun handleControlMessage(
    rawMessage: AirbyteMessage,
    origin: AirbyteMessageOrigin,
  ) {
    if (rawMessage.type == AirbyteMessage.Type.CONTROL) {
      eventPublisher.publishEvent(ReplicationAirbyteMessageEvent(origin, rawMessage, context.replicationContext))
    }
  }

  @VisibleForTesting
  fun internalProcessMessageFromDestination(destinationRawMessage: AirbyteMessage) {
    streamStatusTracker.track(destinationRawMessage)
    logger.debug { "State from destination: $destinationRawMessage" }
    messageTracker.acceptFromDestination(destinationRawMessage)
    if (isAnalyticsMessage(destinationRawMessage)) {
      analyticsTracker.addMessage(destinationRawMessage, AirbyteMessageOrigin.DESTINATION)
    }
    if (destinationRawMessage.type == STATE) {
      metricClient.count(metric = OssMetricsRegistry.STATE_PROCESSED_FROM_DESTINATION, attributes = metricAttrs.toTypedArray())
    }
    handleControlMessage(destinationRawMessage, AirbyteMessageOrigin.DESTINATION)
  }

  fun processMessageFromSource(sourceRawMessage: AirbyteMessage): Optional<AirbyteMessage> =
    internalProcessMessageFromSource(attachIdToStateMessageFromSource(sourceRawMessage))
      ?.let { mapper?.mapMessage(it) ?: it }
      ?.let { Optional.of(it) } ?: Optional.empty()

  internal fun applyTransformationMappers(message: AirbyteJsonRecordAdapter) {
    streamMappers[message.streamDescriptor]?.takeIf { it.isNotEmpty() }?.let { mappers ->
      recordMapper.applyMappers(message, mappers)
    }
  }

  private fun getTotalStats(
    timeTracker: ThreadedTimeTracker,
    completed: Boolean,
  ): SyncStats =
    syncStatsTracker.getTotalStats(completed).apply {
      replicationStartTime = timeTracker.replicationStartTime
      replicationEndTime = timeTracker.replicationEndTime
      sourceReadStartTime = timeTracker.sourceReadStartTime
      sourceReadEndTime = timeTracker.sourceReadEndTime
      destinationWriteStartTime = timeTracker.destinationWriteStartTime
      destinationWriteEndTime = timeTracker.destinationWriteEndTime
    }

  private fun getFailureReasons(
    replicationFailures: List<FailureReason>,
    output: ReplicationOutput,
  ): List<FailureReason> {
    val failures =
      mutableListOf<FailureReason>().apply {
        addAll(messageTracker.errorTraceMessageFailure(context.replicationContext.jobId, context.replicationContext.attempt))
        addAll(replicationFailures)
      }
    if (failures.isNotEmpty()) {
      output.failures = failures
    }
    return failures
  }

  private fun buildPerformanceMetrics(performanceMetrics: PerformanceMetrics?): PerformanceMetrics =
    PerformanceMetrics().apply {
      performanceMetrics?.additionalProperties?.forEach { (key, value) ->
        setAdditionalProperty(key, value)
      }
      val mapperMetrics = recordMapper.collectStopwatches()
      if (mapperMetrics.isNotEmpty()) {
        setAdditionalProperty("mappers", mapperMetrics)
      }
    }

  private fun updateRecordsCount() {
    recordsRead++

    if (recordsRead == 5000L) {
      totalRecordsRead += recordsRead
      logger.info {
        val bytes = byteCountToDisplaySize(syncStatsTracker.getTotalBytesEmitted())
        "Records read: $totalRecordsRead ($bytes)"
      }
      recordsRead = 0
    }
  }
}

private fun isAnalyticsMessage(msg: AirbyteMessage): Boolean =
  msg.type == AirbyteMessage.Type.TRACE && msg.trace.type == AirbyteTraceMessage.Type.ANALYTICS
