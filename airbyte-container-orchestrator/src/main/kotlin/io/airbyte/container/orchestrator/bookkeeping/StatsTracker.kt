/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.bookkeeping

import com.google.common.hash.HashFunction
import com.google.common.util.concurrent.AtomicDouble
import io.airbyte.commons.json.Jsons
import io.airbyte.config.FileTransferInformations
import io.airbyte.container.orchestrator.worker.model.getIdFromStateMessage
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.protocol.models.v0.AirbyteEstimateTraceMessage
import io.airbyte.protocol.models.v0.AirbyteRecordMessage
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStreamNameNamespacePair
import io.github.oshai.kotlinlogging.KotlinLogging
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.LongAccumulator

/**
 * Track Stats for a specific stream.
 *
 * Most stats are tracked on the StreamStatsCounters instance as the messages or states ar emitted.
 *
 * In order to track the number of committed records, we count emitted records in between states. To
 * do so, we track messages in a EmittedStatsCounters, when we see a state message from a source, we
 * add the current (State, EmittedStatsCounters) to a list. When we see a state message back from
 * the destination, we pop the corresponding EmittedStatsCounters and update the global committed
 * records count.
 *
 * Data class for tracking stats of a given stream, this is also how stats are returned to the outside.
 *
 * All the counters are implemented as AtomicLong to avoid race conditions on updates.
 *
 * TODO: Make internal when [io.airbyte.container.orchestrator.bookkeeping.ParallelStreamStatsTracker] has converted.
 */
data class StreamStatsCounters(
  val emittedRecordsCount: AtomicLong = AtomicLong(),
  val filteredOutRecords: AtomicLong = AtomicLong(),
  val filteredOutBytesCount: AtomicLong = AtomicLong(),
  val emittedBytesCount: AtomicLong = AtomicLong(),
  val committedRecordsCount: AtomicLong = AtomicLong(),
  val committedBytesCount: AtomicLong = AtomicLong(),
  val estimatedRecordsCount: AtomicLong = AtomicLong(),
  val estimatedBytesCount: AtomicLong = AtomicLong(),
  val sourceStateCount: AtomicLong = AtomicLong(),
  val destinationStateCount: AtomicLong = AtomicLong(),
  val maxSecondsToReceiveState: LongAccumulator = LongAccumulator(Math::max, 0),
  val meanSecondsToReceiveState: AtomicDouble = AtomicDouble(),
  val maxSecondsBetweenStateEmittedAndCommitted: LongAccumulator = LongAccumulator(Math::max, 0),
  val meanSecondsBetweenStateEmittedAndCommitted: AtomicDouble = AtomicDouble(),
  val unreliableStateOperations: AtomicBoolean = AtomicBoolean(false),
  val rejectedRecordsCount: AtomicLong = AtomicLong(),
)

/**
 * Data class for tracking Emitted stats.
 *
 * EmittedStatsCounters are the stats tied to a State that hasn't been acked back from the
 * destination yet. Those stats are "emitted". They will eventually add up to the committed stats
 * once the state is acked by the destination.
 */
data class EmittedStatsCounters(
  val remittedRecordsCount: AtomicLong = AtomicLong(),
  val emittedBytesCount: AtomicLong = AtomicLong(),
  val filteredOutRecords: AtomicLong = AtomicLong(),
  val filteredOutBytesCount: AtomicLong = AtomicLong(),
)

/**
 * Data class for tracking stats that have been staged and are waiting for the state ack from the
 * destination to become committed.
 *
 * This record should track the number of records/bytes emitted that are associated to a given
 * state.
 */
private data class StagedStats(
  val stateId: Int,
  val stateMessage: AirbyteStateMessage,
  val emittedStatsCounters: EmittedStatsCounters,
  val receivedTime: LocalDateTime,
)

private val logger = KotlinLogging.logger { }

private const val DEST_EMITTED_BYTES_COUNT = "emittedBytesCount"

private const val DEST_COMMITTED_RECORDS_COUNT = "committedRecordsCount"

private const val DEST_EMITTED_RECORDS_COUNT = "emittedRecordsCount"

private const val DEST_COMMITTED_BYTES_COUNT = "committedBytesCount"

private const val DEST_REJECTED_RECORDS_COUNT = "rejectedRecordsCount"

/**
 * Track Stats for a specific stream.
 * <p>
 * Most stats are tracked on the StreamStatsCounters instance as the messages or states ar emitted.
 * <p>
 * In order to track the number of committed records, we count emitted records in between states. To
 * do so, we track messages in a EmittedStatsCounters, when we see a state message from a source, we
 * add the current (State, EmittedStatsCounters) to a list. When we see a state message back from
 * the destination, we pop the corresponding EmittedStatsCounters and update the global committed
 * records count.
 */
class StreamStatsTracker(
  val nameNamespacePair: AirbyteStreamNameNamespacePair,
  private val metricClient: MetricClient,
  private val isBookkeeperMode: Boolean,
) {
  val streamStats = StreamStatsCounters()
  private val stateIds = ConcurrentHashMap.newKeySet<Int>()
  private val stagedStatsList = ConcurrentLinkedQueue<StagedStats>()
  private var emittedStats = EmittedStatsCounters()
  private var previousEmittedStats = EmittedStatsCounters()
  private var previousStateMessageReceivedAt: LocalDateTime? = null

  fun updateFilteredOutRecordsStats(recordMessage: AirbyteRecordMessage) {
    val emittedStatsToUpdate = emittedStats
    val filteredOutByteSize = Jsons.getEstimatedByteSize(recordMessage.data).toLong()
    with(emittedStatsToUpdate) {
      filteredOutRecords.incrementAndGet()
      filteredOutBytesCount.addAndGet(filteredOutByteSize)
    }
    with(streamStats) {
      filteredOutRecords.incrementAndGet()
      filteredOutBytesCount.addAndGet(filteredOutByteSize)
    }
  }

  fun trackRecordCountFromDestination(recordMessage: AirbyteRecordMessage) {
    if (!isBookkeeperMode) {
      return
    }
    logger.debug { "Dummy stats message from destination $recordMessage" }
    val byteCount =
      recordMessage.additionalProperties[DEST_EMITTED_BYTES_COUNT]
        .asLongOrZero()

    val recordCount =
      recordMessage.additionalProperties[DEST_EMITTED_RECORDS_COUNT]
        .asLongOrZero()

    emittedStats.apply {
      remittedRecordsCount.set(recordCount)
      emittedBytesCount.set(byteCount)
    }

    streamStats.apply {
      emittedRecordsCount.set(recordCount)
      emittedBytesCount.set(byteCount)
    }
  }

  private fun Any?.asLongOrZero(): Long = (this as? Number)?.toLong() ?: 0L

  /**
   * Bookkeeping for when a record message is read.
   *
   * We update emitted records count on both emittedStats and streamStats. emittedStats is the tracker
   * for what is going to become committed once the state is acked. We update the global count to
   * avoid having to traverse the map to get the global count.
   */
  fun trackRecord(recordMessage: AirbyteRecordMessage) {
    val fileSize = getFileSize(recordMessage)
    val bytesToTrack = fileSize ?: getRecordSize(recordMessage)

    // Update the current emitted stats
    // We do a local copy of the reference to emittedStats to ensure all the stats are
    // updated on the same instance in case the stats were to be staged. This would happen
    // if trackStateFromSource were called in parallel. The alternative would be to use a
    // ReadWriteLock where trackRecord would acquire the read and trackStateFromSource would
    // acquire the write.
    val emittedStatsToUpdate = emittedStats
    with(emittedStatsToUpdate) {
      remittedRecordsCount.incrementAndGet()
      emittedBytesCount.addAndGet(bytesToTrack)
    }

    // Update the global stream stats
    with(streamStats) {
      emittedRecordsCount.incrementAndGet()
      emittedBytesCount.addAndGet(bytesToTrack)
    }
  }

  private fun getFileSize(recordMessage: AirbyteRecordMessage): Long? =
    if (recordMessage.fileReference != null) {
      recordMessage.fileReference.fileSizeBytes
    } else if (recordMessage.additionalProperties != null) {
      // TODO: we can probably wrap this in an extension method and encapsulate the keys somewhere as constants.
      recordMessage.additionalProperties["file"]?.let {
        logger.info { "Received a file transfer record: $it" }
        val fileTransferInformations = Jsons.deserialize(Jsons.serialize(it), FileTransferInformations::class.java)
        fileTransferInformations.bytes
      }
    } else {
      null
    }

  private fun getRecordSize(recordMessage: AirbyteRecordMessage): Long = Jsons.getEstimatedByteSize(recordMessage.data).toLong()

  /**
   * Bookkeeping for when a state is read from the source.
   *
   * Reading a state from the source means that we need to stage our current counters. All the
   * in-flight records since the last state are associated to this state and will be kept aside until
   * we see that same state back from the destination. We should also recreate a new set of counters
   * to keep on tracking incoming messages.
   */
  fun trackStateFromSource(stateMessage: AirbyteStateMessage) {
    if (isBookkeeperMode) {
      logger.debug { "State message from source $stateMessage" }
    }
    val currentTime = LocalDateTime.now()
    streamStats.sourceStateCount.incrementAndGet()

    if (streamStats.unreliableStateOperations.get()) {
      // State collision previously detected, we skip all operations that involve state tracking.
      return
    }

    val stateId: Int = stateMessage.getStateIdForStatsTracking()
    if (!this.stateIds.add(stateId)) {
      // State collision detected, it means that state tracking is compromised for this stream.
      // Rather than reporting incorrect data, we skip all operations that involve state tracking.
      streamStats.unreliableStateOperations.set(true)

      // We can clear the stagedStatsList since we won't be processing it anymore.
      stagedStatsList.clear()
      logger.info {
        "State collision detected for stream name(${nameNamespacePair.name}), stream namespace(${nameNamespacePair.namespace})"
      }
      metricClient.count(metric = OssMetricsRegistry.STATE_ERROR_COLLISION_FROM_SOURCE)
      return
    }

    // Rollover stat bucket
    previousEmittedStats = emittedStats
    emittedStats = EmittedStatsCounters()

    stagedStatsList.add(StagedStats(stateId, stateMessage, previousEmittedStats, currentTime))

    // Updating state checkpointing metrics
    // previousStateMessageReceivedAt is null when it's the first state message of a stream.
    previousStateMessageReceivedAt?.let {
      val timeSinceLastState: Long = it.until(currentTime, ChronoUnit.SECONDS)
      streamStats.maxSecondsToReceiveState.accumulate(timeSinceLastState)

      // We are measuring intervals in-between states, so there's going to be one less interval than the
      // number of states.
      streamStats.meanSecondsToReceiveState.set(
        updateMean(
          previousMean = streamStats.meanSecondsToReceiveState.get(),
          previousCount = streamStats.sourceStateCount.get() - 1,
          newDataPoint = timeSinceLastState.toDouble(),
        ),
      )
    }

    previousStateMessageReceivedAt = currentTime
  }

  /**
   * Bookkeeping for when we see a state from a destination.
   *
   * The current contract with a destination is: a state message means that all records preceding this
   * state message have been persisted, destination may skip state message.
   * <p>
   * We will un-queue all the StagedStats and add them to the global counters as committed until the
   * said acked state.
   */
  fun trackStateFromDestination(stateMessage: AirbyteStateMessage) {
    if (isBookkeeperMode) {
      logger.debug { "State message from destination : $stateMessage" }
    }
    val currentTime = LocalDateTime.now()
    streamStats.destinationStateCount.incrementAndGet()

    if (streamStats.unreliableStateOperations.get()) {
      // State collision previously detected, we skip all operations that involve state tracking.
      return
    }

    val stateId: Int = stateMessage.getStateIdForStatsTracking()
    if (!stateIds.contains(stateId)) {
      metricClient.count(metric = OssMetricsRegistry.STATE_ERROR_UNKNOWN_FROM_DESTINATION)
      logger.warn {
        "Unexpected state from destination for stream ${nameNamespacePair.namespace}:${nameNamespacePair.name}, " +
          "$stateId not found in the stored stateIds"
      }
      return
    } else if (stagedStatsList.isEmpty()) {
      metricClient.count(metric = OssMetricsRegistry.STATE_ERROR_UNKNOWN_FROM_DESTINATION)
      logger.warn {
        "Unexpected state from destination for stream ${nameNamespacePair.namespace}:${nameNamespacePair.name}, " +
          "stagedStatsList is empty"
      }
      return
    }

    logger.debug { "Id of the state message received from the destination $stateId" }

    var stagedStats: StagedStats? = null
    // un-stage stats until the stateMessage
    while (!stagedStatsList.isEmpty()) {
      stagedStats = stagedStatsList.poll()
      logger.debug {
        "removing ${stagedStats.stateId} from the stored stateIds for the stream " +
          "${nameNamespacePair.namespace}:${nameNamespacePair.name}, " +
          "state received time ${stagedStats.receivedTime}" +
          "stagedStatsList size after poll: ${stagedStatsList.size}, " +
          "stateIds size before removal ${stateIds.size}"
      }
      // Cleaning up stateIds as we go to avoid un-staging on duplicate or our of order state messages
      stateIds.remove(stagedStats.stateId)

      if (isBookkeeperMode) {
        val (totalCommittedRecords, totalCommittedBytes, totalRejectedRecords) =
          extractBookkeeperRelatedCounts(stateMessage)
        streamStats.apply {
          if (emittedRecordsCount.get() < totalCommittedRecords) {
            emittedRecordsCount.set(totalCommittedRecords)
          }
          if (emittedBytesCount.get() < totalCommittedBytes) {
            emittedBytesCount.set(totalCommittedBytes)
          }
          committedRecordsCount.set(totalCommittedRecords)
          committedBytesCount.set(totalCommittedBytes)
          rejectedRecordsCount.set(totalRejectedRecords)
          // TODO(Subodh): Once destination starts applying mappers, channel the filtered records count` as well
          filteredOutRecords.set(0)
          filteredOutBytesCount.set(0)
        }
      } else {
        // Increment committed stats as we are un-staging stats
        streamStats.committedBytesCount.addAndGet(
          stagedStats.emittedStatsCounters.emittedBytesCount
            .get()
            .minus(stagedStats.emittedStatsCounters.filteredOutBytesCount.get()),
        )
        streamStats.committedRecordsCount.addAndGet(
          stagedStats.emittedStatsCounters.remittedRecordsCount
            .get()
            .minus(stagedStats.emittedStatsCounters.filteredOutRecords.get()),
        )

        // If rejected records, we should decrement the record count and increment rejected
        // The destinationStats contains the recordCount however we're continuing to the "record" count from the platform for
        // checksum purpose. Otherwise, it would be the same as just trusting counts from the destination.
        // RejectedRecords is an edge case because the counter is the only way for the platform to be aware of record rejection.
        if (stagedStats.stateId == stateId) {
          stateMessage.destinationStats?.rejectedRecordCount?.let { rejectedCount ->
            if (rejectedCount > 0) {
              val rejectedCountLong = rejectedCount.toLong()
              streamStats.rejectedRecordsCount.addAndGet(rejectedCountLong)
              streamStats.committedRecordsCount.addAndGet(0 - rejectedCountLong)
            }
          }
        }
      }

      if (stagedStats.stateId == stateId) {
        break
      }
    }

    // Updating state checkpointing metrics
    stagedStats?.receivedTime?.until(currentTime, ChronoUnit.SECONDS)?.let { durationBetweenStateEmittedAndCommitted ->
      streamStats.maxSecondsBetweenStateEmittedAndCommitted.accumulate(durationBetweenStateEmittedAndCommitted)
      streamStats.meanSecondsBetweenStateEmittedAndCommitted.set(
        updateMean(
          previousMean = streamStats.meanSecondsBetweenStateEmittedAndCommitted.get(),
          previousCount = streamStats.destinationStateCount.get() - 1,
          newDataPoint = durationBetweenStateEmittedAndCommitted.toDouble(),
        ),
      )
    }
  }

  private fun extractBookkeeperRelatedCounts(stateMessage: AirbyteStateMessage) =
    when (stateMessage.type!!) {
      AirbyteStateMessage.AirbyteStateType.GLOBAL -> {
        val targetStreamState =
          stateMessage.global.streamStates.find { streamState ->
            streamState.streamDescriptor?.let { descriptor ->
              descriptor.namespace == nameNamespacePair.namespace &&
                descriptor.name == nameNamespacePair.name
            } ?: false
          }

        targetStreamState?.additionalProperties?.let(::extractCounts)
          ?: Triple(0L, 0L, 0L)
      }

      AirbyteStateMessage.AirbyteStateType.STREAM -> {
        extractCounts(stateMessage.additionalProperties)
      }

      AirbyteStateMessage.AirbyteStateType.LEGACY -> {
        error("Bookkeeper mode doesn't work with LEGACY state message")
      }
    }

  private fun extractCounts(additionalProperties: Map<String, Any>): Triple<Long, Long, Long> =
    Triple(
      (additionalProperties[DEST_COMMITTED_RECORDS_COUNT] as? Number)?.toLong() ?: 0,
      (additionalProperties[DEST_COMMITTED_BYTES_COUNT] as? Number)?.toLong() ?: 0,
      (additionalProperties[DEST_REJECTED_RECORDS_COUNT] as? Number)?.toLong() ?: 0,
    )

  /**
   * Bookkeeping for when we see an estimate message.
   */
  fun trackEstimates(msg: AirbyteEstimateTraceMessage): Unit =
    with(streamStats) {
      estimatedBytesCount.set(msg.byteEstimate)
      estimatedRecordsCount.set(msg.rowEstimate)
    }

  fun getTrackedEmittedRecordsSinceLastStateMessage(): Long = previousEmittedStats.remittedRecordsCount.get()

  fun getTrackedEmittedRecordsSinceLastStateMessage(stateMessage: AirbyteStateMessage): Long {
    val stateId = stateMessage.getStateIdForStatsTracking()
    val stagedStats: StagedStats? = stagedStatsList.find { it.stateId == stateId }
    if (stagedStats == null) {
      logger.warn { "Could not find the state message with id $stateId in the stagedStatsList" }
    }
    return stagedStats?.emittedStatsCounters?.remittedRecordsCount?.get() ?: 0
  }

  fun getTrackedFilteredOutRecordsSinceLastStateMessage(stateMessage: AirbyteStateMessage): Long {
    val stateId = stateMessage.getStateIdForStatsTracking()
    val stagedStats: StagedStats? = stagedStatsList.find { it.stateId == stateId }
    if (stagedStats == null) {
      logger.warn { "Could not find the state message with id $stateId in the stagedStatsList" }
    }
    return stagedStats?.emittedStatsCounters?.filteredOutRecords?.get() ?: 0
  }

  fun areStreamStatsReliable(): Boolean = !streamStats.unreliableStateOperations.get()
}

fun AirbyteStateMessage.getStateHashCode(hashFunction: HashFunction): Int =
  when (type) {
    AirbyteStateMessage.AirbyteStateType.GLOBAL -> hashFunction.hashBytes(Jsons.serialize(global).toByteArray()).hashCode()
    AirbyteStateMessage.AirbyteStateType.STREAM -> hashFunction.hashBytes(Jsons.serialize(stream.streamState).toByteArray()).hashCode()
    // state type is legacy
    else -> hashFunction.hashBytes(Jsons.serialize(data).toByteArray()).hashCode()
  }

fun AirbyteStateMessage.getStateIdForStatsTracking(): Int = getIdFromStateMessage(this)

private fun updateMean(
  previousMean: Double,
  previousCount: Long,
  newDataPoint: Double,
): Double {
  val newCount = previousCount + 1
  return ((previousMean * previousCount) + newDataPoint) / newCount
}
