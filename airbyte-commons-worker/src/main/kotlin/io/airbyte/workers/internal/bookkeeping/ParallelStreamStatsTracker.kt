package io.airbyte.workers.internal.bookkeeping

import io.airbyte.config.StreamSyncStats
import io.airbyte.config.SyncStats
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.protocol.models.AirbyteEstimateTraceMessage
import io.airbyte.protocol.models.AirbyteEstimateTraceMessage.Type
import io.airbyte.protocol.models.AirbyteRecordMessage
import io.airbyte.protocol.models.AirbyteStateMessage
import io.airbyte.protocol.models.AirbyteStreamNameNamespacePair
import io.airbyte.protocol.models.StreamDescriptor
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Prototype
import jakarta.inject.Named
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

private val logger = KotlinLogging.logger { }

private data class SyncStatsCounters(
  val estimatedRecordCount: AtomicLong = AtomicLong(),
  val estimatedBytesCount: AtomicLong = AtomicLong(),
)

@Prototype
@Named("parallelStreamStatsTracker")
class ParallelStreamStatsTracker(private val metricClient: MetricClient) : SyncStatsTracker {
  private val streamTrackers: MutableMap<AirbyteStreamNameNamespacePair, StreamStatsTracker> = ConcurrentHashMap()
  private val syncStatsCounters = SyncStatsCounters()
  private var expectedEstimateType: Type? = null

  @Volatile
  private var hasEstimatesErrors = false

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

  override fun updateSourceStatesStats(
    stateMessage: AirbyteStateMessage,
    trackCommittedStatsWhenUsingGlobalState: Boolean,
  ) {
    if (trackCommittedStatsWhenUsingGlobalState) {
      when (stateMessage.type) {
        AirbyteStateMessage.AirbyteStateType.GLOBAL -> {
          stateMessage.global.streamStates.forEach {
            getOrCreateStreamStatsTracker(getNameNamespacePair(it.streamDescriptor))
              .trackStateFromSource(stateMessage)
          }
        }
        else -> {
          getOrCreateStreamStatsTracker(getNameNamespacePair(stateMessage))
            .trackStateFromSource(stateMessage)
        }
      }
    } else {
      getOrCreateStreamStatsTracker(getNameNamespacePair(stateMessage))
        .trackStateFromSource(stateMessage)
    }
  }

  override fun updateDestinationStateStats(
    stateMessage: AirbyteStateMessage,
    trackCommittedStatsWhenUsingGlobalState: Boolean,
  ) {
    if (trackCommittedStatsWhenUsingGlobalState) {
      when (stateMessage.type) {
        AirbyteStateMessage.AirbyteStateType.GLOBAL -> {
          stateMessage.global.streamStates.forEach {
            getOrCreateStreamStatsTracker(getNameNamespacePair(it.streamDescriptor))
              .trackStateFromDestination(stateMessage)
          }
        }
        else -> {
          getOrCreateStreamStatsTracker(getNameNamespacePair(stateMessage))
            .trackStateFromDestination(stateMessage)
        }
      }
    } else {
      getOrCreateStreamStatsTracker(getNameNamespacePair(stateMessage))
        .trackStateFromDestination(stateMessage)
    }
  }

  /**
   * Return [SyncStats] for the sync. SyncStats is the sum of the stats of all the streams.
   *
   * @param hasReplicationCompleted defines whether a stream has completed. If the stream has
   *        completed, emitted counts/bytes will be used as committed counts/bytes.
   * TODO make internal?
   */
  fun getTotalStats(hasReplicationCompleted: Boolean = false): SyncStats {
    // For backwards compatibility with existing code which treats null and 0 differently,
    // if [getAllStreamSyncStats] is empty then treat it as a null itself.
    // [sumOf] methods handle null values as 0, which is a change that we don't want to make at this time.
    val streamSyncStats = getAllStreamSyncStats(hasReplicationCompleted).takeIf { it.isNotEmpty() }
    val bytesCommitted = streamSyncStats?.sumOf { it.stats.bytesCommitted }
    val recordsCommitted = streamSyncStats?.sumOf { it.stats.recordsCommitted }
    val bytesEmitted = streamSyncStats?.sumOf { it.stats.bytesEmitted }
    val recordsEmitted = streamSyncStats?.sumOf { it.stats.recordsEmitted }
    val estimatedBytes =
      if (!hasEstimatesErrors && expectedEstimateType == Type.SYNC) {
        syncStatsCounters.estimatedBytesCount.get()
      } else {
        streamSyncStats?.mapNotNull { it.stats.estimatedBytes }
          // mapNotNull with return an empty list if there are no non-null results
          // we want to treat an empty list as null for backwards compatability reasons,
          // specifically around treating a 0 and null differently
          ?.takeIf { it.isNotEmpty() }
          ?.sumOf { it }
      }
    val estimatedRecords =
      if (!hasEstimatesErrors && expectedEstimateType == Type.SYNC) {
        syncStatsCounters.estimatedRecordCount.get()
      } else {
        streamSyncStats?.mapNotNull { it.stats.estimatedRecords }
          // mapNotNull with return an empty list if there are no non-null results
          // we want to treat an empty list as null for backwards compatability reasons,
          // specifically around treating a 0 and null differently
          ?.takeIf { it.isNotEmpty() }
          ?.sumOf { it }
      }

    return SyncStats()
      .withBytesCommitted(bytesCommitted)
      .withRecordsCommitted(recordsCommitted)
      .withBytesEmitted(bytesEmitted)
      .withRecordsEmitted(recordsEmitted)
      .withEstimatedBytes(estimatedBytes)
      .withEstimatedRecords(estimatedRecords)
  }

  /**
   * Return all the [StreamSyncStats] for the sync.
   *
   * @param hasReplicationCompleted defines whether a stream has completed. If the stream has
   *        completed, emitted counts/bytes will be used as committed counts/bytes.
   * TODO make internal?
   */
  fun getAllStreamSyncStats(hasReplicationCompleted: Boolean): List<StreamSyncStats> {
    return streamTrackers.values
      // null name means that those are stats from global states or legacy states. We should not
      // report them as stream stats because a null stream doesn't exist.
      // We should still track them as this is how we track stats for legacy states, but they
      // should end up being reported as global sync stats only.
      .filter { it.nameNamespacePair.name != null }
      .map { it.toStreamSyncStats(hasReplicationCompleted) }
  }

  override fun getStreamToCommittedBytes(): Map<AirbyteStreamNameNamespacePair, Long> =
    streamTrackers
      .filterValues { it.nameNamespacePair.name != null }
      .mapValues { it.value.streamStats.committedBytesCount.get() }

  override fun getStreamToCommittedRecords(): Map<AirbyteStreamNameNamespacePair, Long> =
    streamTrackers
      .filterValues { it.nameNamespacePair.name != null }
      .mapValues { it.value.streamStats.committedRecordsCount.get() }

  override fun getStreamToEmittedBytes(): Map<AirbyteStreamNameNamespacePair, Long> =
    streamTrackers
      .filterValues { it.nameNamespacePair.name != null }
      .mapValues { it.value.streamStats.emittedBytesCount.get() }

  override fun getStreamToEmittedRecords(): Map<AirbyteStreamNameNamespacePair, Long> =
    streamTrackers
      .filterValues { it.nameNamespacePair.name != null }
      .mapValues { it.value.streamStats.emittedRecordsCount.get() }

  override fun getStreamToEstimatedRecords(): Map<AirbyteStreamNameNamespacePair, Long> =
    if (hasEstimatesErrors) {
      mapOf()
    } else {
      streamTrackers
        .filterValues { it.nameNamespacePair.name != null }
        .mapValues { it.value.streamStats.estimatedRecordsCount.get() }
    }

  override fun getStreamToEstimatedBytes(): Map<AirbyteStreamNameNamespacePair, Long> =
    if (hasEstimatesErrors) {
      mapOf()
    } else {
      streamTrackers
        .filterValues { it.nameNamespacePair.name != null }
        .mapValues { it.value.streamStats.estimatedBytesCount.get() }
    }

  override fun getTotalRecordsEmitted(): Long = getTotalStats().recordsEmitted ?: 0

  override fun getTotalRecordsEstimated(): Long = getTotalStats().estimatedRecords ?: 0

  override fun getTotalBytesEmitted(): Long = getTotalStats().bytesEmitted ?: 0

  override fun getTotalBytesEstimated(): Long = getTotalStats().estimatedBytes ?: 0

  override fun getTotalBytesCommitted(): Long? = getTotalStats().bytesCommitted

  override fun getTotalRecordsCommitted(): Long? = getTotalStats().recordsCommitted

  override fun getTotalSourceStateMessagesEmitted(): Long = streamTrackers.values.sumOf { it.streamStats.sourceStateCount.get() }

  override fun getTotalDestinationStateMessagesEmitted(): Long = streamTrackers.values.sumOf { it.streamStats.destinationStateCount.get() }

  override fun getMaxSecondsToReceiveSourceStateMessage(): Long =
    streamTrackers.values
      .maxOfOrNull { it.streamStats.destinationStateCount.get() }
      ?: 0

  override fun getMeanSecondsToReceiveSourceStateMessage(): Long =
    computeMean(
      getMean = { it.streamStats.maxSecondsToReceiveState.get().toDouble() },
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

  private fun hasSourceStateErrors(): Boolean = streamTrackers.any { it.value.streamStats.unreliableStateOperations.get() }

  /**
   * Converts a [StreamStatsTracker] into a [StreamSyncStats].
   */
  private fun StreamStatsTracker.toStreamSyncStats(hasReplicationCompleted: Boolean): StreamSyncStats {
    // to avoid having to do `this@toStreamSyncStats.streamStats` within the apply function
    val streamStats = this.streamStats

    return StreamSyncStats()
      .withStreamName(nameNamespacePair.name)
      .withStreamNamespace(nameNamespacePair.namespace)
      .withStats(
        SyncStats()
          .withBytesEmitted(streamStats.emittedBytesCount.get())
          .withRecordsEmitted(streamStats.emittedRecordsCount.get())
          .apply {
            if (hasReplicationCompleted) {
              withBytesCommitted(streamStats.emittedBytesCount.get())
              withRecordsCommitted(streamStats.emittedRecordsCount.get())
            } else {
              withBytesCommitted(streamStats.committedBytesCount.get())
              withRecordsCommitted(streamStats.committedRecordsCount.get())
            }

            if (hasEstimatesErrors) {
              withEstimatedBytes(null)
              withEstimatedRecords(null)
            } else {
              withEstimatedBytes(streamStats.estimatedBytesCount.get())
              withEstimatedRecords(streamStats.estimatedRecordsCount.get())
            }
          },
      )
  }

  /**
   * Get the [StreamStatsTracker] for a given stream. If this tracker doesn't exist, create it.
   */
  private fun getOrCreateStreamStatsTracker(pair: AirbyteStreamNameNamespacePair): StreamStatsTracker {
    // if a entry already exists, return it
    streamTrackers[pair]?.let { return it }

    // We want to avoid multiple threads trying to create a new StreamStatsTracker.
    // This operation should be fairly rare, once per stream, so the synchronized block shouldn't cause
    // too much contention.
    synchronized(this) {
      // Making sure the stream hasn't been created since the previous check.
      streamTrackers[pair]?.let { return it }
      // if no existing tracker exists, create a new one and also place it into the trackers map
      return StreamStatsTracker(pair, metricClient).also { streamTrackers[pair] = it }
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
  ): Long {
    return streamTrackers.values
      .map {
        val count = getCount(it)
        val mean = getMean(it) * count
        Pair(mean, count)
      }
      .reduceOrNull { a, b -> Pair(a.first + b.first, a.second + b.second) }
      ?.let {
        // avoid potential divide by zero error
        if (it.second >= 0) {
          it.first / it.second
        } else {
          0
        }
      }
      ?.toLong()
      ?: 0
  }
}

/**
 * Function for converting various AirbyteMessage types into [AirbyteStreamNameNamespacePair]
 *
 * @throws [IllegalStateException] if an unsupported class is passed.
 */
private inline fun <reified T : Any> getNameNamespacePair(type: T): AirbyteStreamNameNamespacePair =
  when (type) {
    is AirbyteRecordMessage -> AirbyteStreamNameNamespacePair(type.stream, type.namespace)
    is AirbyteEstimateTraceMessage -> AirbyteStreamNameNamespacePair(type.name, type.namespace)
    is AirbyteStateMessage ->
      type.stream?.streamDescriptor
        ?.let { getNameNamespacePair(it) }
        ?: AirbyteStreamNameNamespacePair(null, null)
    else -> throw IllegalArgumentException("Unsupported type ${type::class.java}")
  }

private fun getNameNamespacePair(streamDescriptor: StreamDescriptor): AirbyteStreamNameNamespacePair =
  AirbyteStreamNameNamespacePair(streamDescriptor.name, streamDescriptor.namespace)
