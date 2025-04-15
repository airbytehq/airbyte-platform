/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general.buffered.worker

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
import io.airbyte.config.adapters.AirbyteJsonRecordAdapter
import io.airbyte.config.adapters.AirbyteRecord
import io.airbyte.mappers.application.RecordMapper
import io.airbyte.mappers.transformations.DestinationCatalogGenerator
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteMessage.Type.RECORD
import io.airbyte.protocol.models.v0.AirbyteMessage.Type.STATE
import io.airbyte.protocol.models.v0.AirbyteTraceMessage
import io.airbyte.workers.WorkerUtils
import io.airbyte.workers.exception.WorkerException
import io.airbyte.workers.general.buffered.worker.BytesSizeHelper.byteCountToDisplaySize
import io.airbyte.workers.helper.StreamStatusCompletionTracker
import io.airbyte.workers.internal.AirbyteDestination
import io.airbyte.workers.internal.AirbyteMapper
import io.airbyte.workers.internal.AirbyteSource
import io.airbyte.workers.internal.AnalyticsMessageTracker
import io.airbyte.workers.internal.FieldSelector
import io.airbyte.workers.internal.bookkeeping.AirbyteMessageOrigin
import io.airbyte.workers.internal.bookkeeping.AirbyteMessageTracker
import io.airbyte.workers.internal.bookkeeping.events.ReplicationAirbyteMessageEvent
import io.airbyte.workers.internal.bookkeeping.events.ReplicationAirbyteMessageEventPublishingHelper
import io.airbyte.workers.internal.bookkeeping.getPerStreamStats
import io.airbyte.workers.internal.bookkeeping.getTotalStats
import io.airbyte.workers.internal.bookkeeping.streamstatus.StreamStatusTracker
import io.airbyte.workers.internal.syncpersistence.SyncPersistence
import io.airbyte.workers.models.StateWithId.attachIdToStateMessageFromSource
import io.airbyte.workers.tracker.ThreadedTimeTracker
import io.github.oshai.kotlinlogging.KotlinLogging
import java.nio.file.Path
import java.util.Optional

private val logger = KotlinLogging.logger {}

class ReplicationWorkerHelperK(
  private val fieldSelector: FieldSelector,
  private val mapper: AirbyteMapper,
  private val messageTracker: AirbyteMessageTracker,
  private val syncPersistence: SyncPersistence,
  private val eventPublisher: ReplicationAirbyteMessageEventPublishingHelper,
  private val timeTracker: ThreadedTimeTracker,
  private val analyticsTracker: AnalyticsMessageTracker,
  private val streamStatusCompletionTracker: StreamStatusCompletionTracker,
  private val streamStatusTracker: StreamStatusTracker,
  private val recordMapper: RecordMapper,
  private val replicationWorkerState: ReplicationWorkerState,
  private val context: ReplicationContextProvider.Context,
  destinationCatalogGenerator: DestinationCatalogGenerator,
) {
  private val streamMappers: Map<StreamDescriptor, List<MapperConfig>>
  private val destinationConfig: WorkerDestinationConfig
  private val mappersConfigured: Boolean

  init {
    analyticsTracker.ctx = context.replicationContext

    streamStatusCompletionTracker.startTracking(context.configuredCatalog, context.supportRefreshes)

    val catalogWithoutInvalidMappers = destinationCatalogGenerator.generateDestinationCatalog(context.configuredCatalog)
    streamMappers = catalogWithoutInvalidMappers.catalog.streams.associate { stream -> stream.streamDescriptor to stream.mappers }
    destinationConfig =
      WorkerUtils.syncToWorkerDestinationConfig(context.replicationInput).apply {
        catalog = mapper.mapCatalog(catalog)
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
        fieldSelector.populateFields(it.catalog)
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
    val bytes = byteCountToDisplaySize(messageTracker.syncStatsTracker.getTotalBytesEmitted())
    logger.info { "Total records read: ($bytes)" }
    fieldSelector.reportMetrics(context.replicationContext.sourceId)
    timeTracker.trackSourceReadEndTime()
  }

  fun endOfDestination() {
    timeTracker.trackDestinationWriteEndTime()
  }

  fun processMessageFromDestination(destinationRawMessage: AirbyteMessage) {
    val message = mapper.revertMap(destinationRawMessage)
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
    val streamStats = messageTracker.syncStatsTracker.getPerStreamStats(completed)

    if (!completed && messageTracker.syncStatsTracker.getUnreliableStateTimingMetrics()) {
      logger.warn { "Unreliable committed record counts; setting committed record stats to null" }
    }

    val summary =
      ReplicationAttemptSummary()
        .withStatus(outputStatus)
        .withRecordsSynced(messageTracker.syncStatsTracker.getTotalRecordsCommitted())
        .withBytesSynced(messageTracker.syncStatsTracker.getTotalBytesCommitted())
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

    LineGobbler.endSection("REPLICATION")
    return output
  }

  @VisibleForTesting
  fun internalProcessMessageFromSource(sourceRawMessage: AirbyteMessage): AirbyteMessage? {
    updateRecordsCount()
    fieldSelector.filterSelectedFields(sourceRawMessage)
    fieldSelector.validateSchema(sourceRawMessage)
    messageTracker.acceptFromSource(sourceRawMessage)
    streamStatusTracker.track(sourceRawMessage)
    return when (sourceRawMessage.type) {
      RECORD -> processRecordMessage(sourceRawMessage)
      STATE -> sourceRawMessage
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
        messageTracker.syncStatsTracker.updateFilteredOutRecordsStats(sourceRawMessage.record)
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
      syncPersistence.accept(context.replicationContext.connectionId, destinationRawMessage.state)
    }
    handleControlMessage(destinationRawMessage, AirbyteMessageOrigin.DESTINATION)
  }

  fun processMessageFromSource(sourceRawMessage: AirbyteMessage): Optional<AirbyteMessage> =
    internalProcessMessageFromSource(attachIdToStateMessageFromSource(sourceRawMessage))
      ?.let { mapper.mapMessage(it) }
      ?.let { Optional.of(it) } ?: Optional.empty()

  private fun applyTransformationMappers(message: AirbyteRecord) {
    streamMappers[message.streamDescriptor]?.takeIf { it.isNotEmpty() }?.let { mappers ->
      recordMapper.applyMappers(message, mappers)
    }
  }

  private fun getTotalStats(
    timeTracker: ThreadedTimeTracker,
    completed: Boolean,
  ): SyncStats =
    messageTracker.syncStatsTracker.getTotalStats(completed).apply {
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
    totalRecordsRead += recordsRead

    if (recordsRead == 5000L) {
      logger.info {
        val bytes = byteCountToDisplaySize(messageTracker.syncStatsTracker.getTotalBytesEmitted())
        "Records read: $totalRecordsRead ($bytes)"
      }
      recordsRead = 0
    }
  }
}

private fun isAnalyticsMessage(msg: AirbyteMessage): Boolean =
  msg.type == AirbyteMessage.Type.TRACE && msg.trace.type == AirbyteTraceMessage.Type.ANALYTICS
