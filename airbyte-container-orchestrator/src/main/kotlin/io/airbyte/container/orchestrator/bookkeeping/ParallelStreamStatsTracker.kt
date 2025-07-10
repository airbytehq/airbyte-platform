/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.bookkeeping

import io.airbyte.config.SyncStats
import io.airbyte.container.orchestrator.worker.context.ReplicationInputFeatureFlagReader
import io.airbyte.featureflag.FailSyncOnInvalidChecksum
import io.airbyte.featureflag.LogStateMsgs
import io.airbyte.metrics.MetricClient
import io.airbyte.protocol.models.v0.AirbyteEstimateTraceMessage
import io.airbyte.protocol.models.v0.AirbyteEstimateTraceMessage.Type
import io.airbyte.protocol.models.v0.AirbyteRecordMessage
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStreamNameNamespacePair
import io.airbyte.protocol.models.v0.AirbyteStreamState
import io.airbyte.protocol.models.v0.StreamDescriptor
import io.airbyte.workers.models.ArchitectureConstants
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

private val logger = KotlinLogging.logger { }

private data class SyncStatsCounters(
  val estimatedRecordCount: AtomicLong = AtomicLong(),
  val estimatedBytesCount: AtomicLong = AtomicLong(),
)

@Singleton
@Named("parallelStreamStatsTracker")
class ParallelStreamStatsTracker(
  private val metricClient: MetricClient,
  private val stateCheckSumEventHandler: StateCheckSumCountEventHandler,
  @Value("\${airbyte.platform-mode}") private val platformMode: String,
) : SyncStatsTracker {
  private val streamTrackers: MutableMap<AirbyteStreamNameNamespacePair, StreamStatsTracker> = ConcurrentHashMap()
  private val syncStatsCounters = SyncStatsCounters()
  private var expectedEstimateType: Type? = null
  private var replicationInputFeatureFlagReader: ReplicationInputFeatureFlagReader? = null
  private val isBookkeeperMode: Boolean = platformMode == ArchitectureConstants.BOOKKEEPER

  @Volatile
  private var hasEstimatesErrors = false

  @Volatile
  private var checksumValidationEnabled = true

  override fun updateFilteredOutRecordsStats(recordMessage: AirbyteRecordMessage) {
    getOrCreateStreamStatsTracker(getNameNamespacePair(recordMessage)).updateFilteredOutRecordsStats(recordMessage)
  }

  override fun updateStatsFromDestination(recordMessage: AirbyteRecordMessage) {
    getOrCreateStreamStatsTracker(getNameNamespacePair(recordMessage))
      .trackRecordCountFromDestination(recordMessage)
  }

  override fun updateStats(recordMessage: AirbyteRecordMessage) {
    getOrCreateStreamStatsTracker(getNameNamespacePair(recordMessage))
      .trackRecord(recordMessage)
  }

  override fun updateEstimates(estimate: AirbyteEstimateTraceMessage) {
    if (hasEstimatesErrors) {
      return
    }

    when {
      expectedEstimateType == null -> expectedEstimateType = estimate.type
      expectedEstimateType != estimate.type -> {
        logger.info { "STREAM and SYNC estimates should not be emitted in the same sync" }
        hasEstimatesErrors = true
        return
      }
    }

    when (estimate.type) {
      Type.STREAM -> getOrCreateStreamStatsTracker(getNameNamespacePair(estimate)).trackEstimates(estimate)
      Type.SYNC -> {
        syncStatsCounters.estimatedBytesCount.set(estimate.byteEstimate)
        syncStatsCounters.estimatedRecordCount.set(estimate.rowEstimate)
      }
      else -> Unit
    }
  }

  override fun updateSourceStatesStats(stateMessage: AirbyteStateMessage) {
    val failOnInvalidChecksum = replicationInputFeatureFlagReader?.read(FailSyncOnInvalidChecksum) ?: false

    when (stateMessage.type) {
      AirbyteStateMessage.AirbyteStateType.GLOBAL -> {
        stateMessage.global.streamStates.forEach {
          logStreamNameIfEnabled(it)
          val statsTracker = getOrCreateStreamStatsTracker(getNameNamespacePair(it.streamDescriptor))
          statsTracker.trackStateFromSource(stateMessage)
          updateChecksumValidationStatus(
            statsTracker.areStreamStatsReliable(),
            AirbyteMessageOrigin.SOURCE,
            getNameNamespacePair(it.streamDescriptor),
          )
        }

        validateGlobalStateChecksum(stateMessage, AirbyteMessageOrigin.SOURCE, failOnInvalidChecksum)
      }
      else -> {
        val statsTracker = getOrCreateStreamStatsTracker(getNameNamespacePair(stateMessage))
        statsTracker.trackStateFromSource(stateMessage)
        updateChecksumValidationStatus(
          statsTracker.areStreamStatsReliable(),
          AirbyteMessageOrigin.SOURCE,
          getNameNamespacePair(stateMessage),
        )
        stateCheckSumEventHandler.validateStateChecksum(
          stateMessage = stateMessage,
          platformRecordCount = statsTracker.getTrackedEmittedRecordsSinceLastStateMessage().toDouble(),
          origin = AirbyteMessageOrigin.SOURCE,
          failOnInvalidChecksum = failOnInvalidChecksum,
          checksumValidationEnabled = checksumValidationEnabled,
          streamPlatformRecordCounts = getStats().mapValues { it.value.recordsEmitted },
        )
      }
    }
  }

  private fun logStreamNameIfEnabled(it: AirbyteStreamState) {
    try {
      if (stateCheckSumEventHandler.logIncomingStreamNames) {
        val nameNamespacePair = getNameNamespacePair(it.streamDescriptor)
        logger.info { "Stream in state message ${nameNamespacePair.namespace}.${nameNamespacePair.name} " }
      }
    } catch (e: Exception) {
      logger.error(e) { "Exception while logging stream name" }
    }
  }

  override fun updateDestinationStateStats(stateMessage: AirbyteStateMessage) {
    val failOnInvalidChecksum = replicationInputFeatureFlagReader?.read(FailSyncOnInvalidChecksum) ?: false

    when (stateMessage.type) {
      AirbyteStateMessage.AirbyteStateType.GLOBAL -> {
        validateGlobalStateChecksum(stateMessage, AirbyteMessageOrigin.DESTINATION, failOnInvalidChecksum)
        stateMessage.global.streamStates.forEach {
          getOrCreateStreamStatsTracker(getNameNamespacePair(it.streamDescriptor))
            .trackStateFromDestination(stateMessage)
        }
      }
      else -> {
        val statsTracker = getOrCreateStreamStatsTracker(getNameNamespacePair(stateMessage))
        val filteredOutRecords = statsTracker.getTrackedFilteredOutRecordsSinceLastStateMessage(stateMessage)
        stateCheckSumEventHandler.validateStateChecksum(
          stateMessage = stateMessage,
          platformRecordCount = statsTracker.getTrackedEmittedRecordsSinceLastStateMessage(stateMessage).toDouble(),
          origin = AirbyteMessageOrigin.DESTINATION,
          failOnInvalidChecksum = failOnInvalidChecksum,
          checksumValidationEnabled = checksumValidationEnabled,
          streamPlatformRecordCounts = getStats().mapValues { it.value.recordsCommitted },
          filteredOutRecords = filteredOutRecords.toDouble(),
        )
        statsTracker.trackStateFromDestination(stateMessage)
      }
    }
  }

  override fun getStats(): Map<AirbyteStreamNameNamespacePair, StreamStats> =
    streamTrackers
      .filterValues { it.nameNamespacePair.name != null }
      .mapValues { StreamStatsView(it.value, hasEstimatesErrors) }

  fun isChecksumValidationEnabled(): Boolean = checksumValidationEnabled

  private fun updateChecksumValidationStatus(
    streamStatsReliable: Boolean,
    origin: AirbyteMessageOrigin,
    streamNameNamespacePair: AirbyteStreamNameNamespacePair,
  ) {
    if (checksumValidationEnabled && !streamStatsReliable) {
      val logMessage =
        "State message checksum validation disabled: " +
          "${origin.name.lowercase()} state message collision detected for " +
          "stream ${streamNameNamespacePair.name}:${streamNameNamespacePair.namespace}."
      logger.warn { logMessage }
      checksumValidationEnabled = false
    }
  }

  private fun validateGlobalStateChecksum(
    stateMessage: AirbyteStateMessage,
    origin: AirbyteMessageOrigin,
    failOnInvalidChecksum: Boolean,
  ) {
    val expectedRecordCount = streamTrackers.values.sumOf { getEmittedCount(origin, stateMessage, it).toDouble() }
    val filteredOutRecords =
      if (origin == AirbyteMessageOrigin.DESTINATION) {
        streamTrackers.values.sumOf { it.getTrackedFilteredOutRecordsSinceLastStateMessage(stateMessage) }
      } else {
        0
      }
    stateCheckSumEventHandler.validateStateChecksum(
      stateMessage = stateMessage,
      platformRecordCount = expectedRecordCount,
      origin = origin,
      failOnInvalidChecksum = failOnInvalidChecksum,
      checksumValidationEnabled = checksumValidationEnabled,
      includeStreamInLogs = false,
      streamPlatformRecordCounts = getStats().mapValues { it.value.recordsEmitted },
      filteredOutRecords = filteredOutRecords.toDouble(),
    )
  }

  private fun getEmittedCount(
    origin: AirbyteMessageOrigin,
    stateMessage: AirbyteStateMessage,
    tracker: StreamStatsTracker,
  ): Long =
    when (origin) {
      AirbyteMessageOrigin.SOURCE -> tracker.getTrackedEmittedRecordsSinceLastStateMessage()
      AirbyteMessageOrigin.DESTINATION -> tracker.getTrackedEmittedRecordsSinceLastStateMessage(stateMessage)
      AirbyteMessageOrigin.INTERNAL -> 0
    }

  /**
   * Return [SyncStats] for the sync. SyncStats is the sum of the stats of all the streams.
   *
   * @param hasReplicationCompleted defines whether a stream has completed. If the stream has
   *        completed, emitted counts/bytes will be used as committed counts/bytes.
   * TODO the notion of hasReplicationCompleted feels obsolete, we should simplify and always return the count
   */
  fun getTotalStats(hasReplicationCompleted: Boolean = false): SyncStats {
    // For backwards compatibility with existing code which treats null and 0 differently,
    // if [getAllStreamSyncStats] is empty then treat it as a null itself.
    // [sumOf] methods handle null values as 0, which is a change that we don't want to make at this time.
    val streamSyncStats = getPerStreamStats(hasReplicationCompleted)
    val estimatedBytes =
      if (!hasEstimatesErrors && expectedEstimateType == Type.SYNC) {
        syncStatsCounters.estimatedBytesCount.get()
      } else {
        streamSyncStats
          .mapNotNull { it.stats.estimatedBytes }
          // mapNotNull with return an empty list if there are no non-null results
          // we want to treat an empty list as null for backwards compatability reasons,
          // specifically around treating a 0 and null differently
          .takeIf { it.isNotEmpty() }
          ?.sumOf { it }
      }
    val estimatedRecords =
      if (!hasEstimatesErrors && expectedEstimateType == Type.SYNC) {
        syncStatsCounters.estimatedRecordCount.get()
      } else {
        streamSyncStats
          .mapNotNull { it.stats.estimatedRecords }
          // mapNotNull with return an empty list if there are no non-null results
          // we want to treat an empty list as null for backwards compatability reasons,
          // specifically around treating a 0 and null differently
          .takeIf { it.isNotEmpty() }
          ?.sumOf { it }
      }

    return getTotalStats(streamSyncStats, hasReplicationCompleted)
      .withEstimatedBytes(estimatedBytes)
      .withEstimatedRecords(estimatedRecords)
      .apply {
        recordsRejected?.let {
          if (it > 0) {
            withRecordsRejected(it)
          }
        }
      }
  }

  override fun getTotalSourceStateMessagesEmitted(): Long = streamTrackers.values.sumOf { it.streamStats.sourceStateCount.get() }

  override fun getTotalDestinationStateMessagesEmitted(): Long = streamTrackers.values.sumOf { it.streamStats.destinationStateCount.get() }

  override fun getMaxSecondsToReceiveSourceStateMessage(): Long =
    streamTrackers.values
      .maxOfOrNull { it.streamStats.destinationStateCount.get() }
      ?: 0

  override fun getMeanSecondsToReceiveSourceStateMessage(): Long =
    computeMean(
      getMean = {
        it.streamStats.maxSecondsToReceiveState
          .get()
          .toDouble()
      },
      getCount = { it.streamStats.sourceStateCount.get() - 1 },
    )

  override fun getMaxSecondsBetweenStateMessageEmittedAndCommitted(): Long? =
    if (hasSourceStateErrors()) {
      null
    } else {
      streamTrackers.values.maxOfOrNull { it.streamStats.maxSecondsBetweenStateEmittedAndCommitted.get() }
    }

  override fun getMeanSecondsBetweenStateMessageEmittedAndCommitted(): Long? =
    if (hasSourceStateErrors()) {
      null
    } else {
      computeMean(
        getMean = { it.streamStats.meanSecondsBetweenStateEmittedAndCommitted.get() },
        getCount = { it.streamStats.destinationStateCount.get() },
      )
    }

  override fun getUnreliableStateTimingMetrics() = hasSourceStateErrors()

  override fun setReplicationFeatureFlagReader(replicationInputFeatureFlagReader: ReplicationInputFeatureFlagReader) {
    this.replicationInputFeatureFlagReader = replicationInputFeatureFlagReader
  }

  override fun endOfReplication(completedSuccessfully: Boolean) {
    stateCheckSumEventHandler.close(completedSuccessfully)
  }

  private fun hasSourceStateErrors(): Boolean =
    streamTrackers.any {
      it.value.streamStats.unreliableStateOperations
        .get()
    }

  /**
   * Get the [StreamStatsTracker] for a given stream. If this tracker doesn't exist, create it.
   */
  private fun getOrCreateStreamStatsTracker(pair: AirbyteStreamNameNamespacePair): StreamStatsTracker {
    // if an entry already exists, return it
    streamTrackers[pair]?.let {
      return it
    }

    // We want to avoid multiple threads trying to create a new StreamStatsTracker.
    // This operation should be fairly rare, once per stream, so the synchronized block shouldn't cause
    // too much contention.
    synchronized(this) {
      // Making sure the stream hasn't been created since the previous check.
      streamTrackers[pair]?.let {
        return it
      }

      if (replicationInputFeatureFlagReader?.read(LogStateMsgs) == true) {
        logger.info { "Creating new stats tracker for stream $pair" }
      }
      // if no existing tracker exists, create a new one and also place it into the trackers map
      return StreamStatsTracker(
        nameNamespacePair = pair,
        metricClient = metricClient,
        isBookkeeperMode = isBookkeeperMode,
      ).also { streamTrackers[pair] = it }
    }
  }

  /**
   * Helper function to compute some global average from the per stream averages.
   *
   * @param getMean how to get the local average from a [StreamStatsTracker]
   * @param getCount how to get the local total count related to that average from a
   *        [StreamStatsTracker]
   * @return the global average
   */
  private fun computeMean(
    getMean: (StreamStatsTracker) -> Double,
    getCount: (StreamStatsTracker) -> Long,
  ): Long =
    streamTrackers.values
      .map {
        val count = getCount(it)
        val mean = getMean(it) * count
        Pair(mean, count)
      }.reduceOrNull { a, b -> Pair(a.first + b.first, a.second + b.second) }
      ?.let {
        // avoid potential divide by zero error
        if (it.second >= 0) {
          it.first / it.second
        } else {
          0
        }
      }?.toLong()
      ?: 0
}

/**
 * Function for converting various AirbyteMessage types into [AirbyteStreamNameNamespacePair]
 *
 * @throws [IllegalStateException] if an unsupported class is passed.
 */
inline fun <reified T : Any> getNameNamespacePair(type: T): AirbyteStreamNameNamespacePair =
  when (type) {
    is AirbyteRecordMessage -> AirbyteStreamNameNamespacePair(type.stream, type.namespace)
    is AirbyteEstimateTraceMessage -> AirbyteStreamNameNamespacePair(type.name, type.namespace)
    is AirbyteStateMessage ->
      type.stream
        ?.streamDescriptor
        ?.let { getNameNamespacePair(it) }
        ?: AirbyteStreamNameNamespacePair(null, null)
    else -> throw IllegalArgumentException("Unsupported type ${type::class.java}")
  }

fun getNameNamespacePair(streamDescriptor: StreamDescriptor): AirbyteStreamNameNamespacePair =
  AirbyteStreamNameNamespacePair(streamDescriptor.name, streamDescriptor.namespace)
