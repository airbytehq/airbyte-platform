package io.airbyte.workers.internal.bookkeeping

import com.google.common.hash.HashFunction
import com.google.common.util.concurrent.AtomicDouble
import io.airbyte.commons.json.Jsons
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.OssMetricsRegistry
import io.airbyte.protocol.models.AirbyteEstimateTraceMessage
import io.airbyte.protocol.models.AirbyteRecordMessage
import io.airbyte.protocol.models.AirbyteStateMessage
import io.airbyte.protocol.models.AirbyteStreamNameNamespacePair
import io.airbyte.workers.models.StateWithId
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
 * TODO: Make internal when [ParallelStreamStatsTracker] has converted.
 */
data class StreamStatsCounters(
  val emittedRecordsCount: AtomicLong = AtomicLong(),
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
  private val logsForStripeChecksumDebugging: Boolean,
) {
  val streamStats = StreamStatsCounters()
  private val stateIds = ConcurrentHashMap.newKeySet<Int>()
  private val stagedStatsList = ConcurrentLinkedQueue<StagedStats>()
  private var emittedStats = EmittedStatsCounters()
  private var previousEmittedStats = EmittedStatsCounters()
  private var previousStateMessageReceivedAt: LocalDateTime? = null
  private var alreadyLogged: Boolean = false

  /**
   * Bookkeeping for when a record message is read.
   *
   * We update emitted records count on both emittedStats and streamStats. emittedStats is the tracker
   * for what is going to become committed once the state is acked. We update the global count to
   * avoid having to traverse the map to get the global count.
   */
  fun trackRecord(recordMessage: AirbyteRecordMessage) {
    val estimatedBytesSize: Long = Jsons.getEstimatedByteSize(recordMessage.data).toLong()

    // Update the current emitted stats
    // We do a local copy of the reference to emittedStats to ensure all the stats are
    // updated on the same instance in case the stats were to be staged. This would happen
    // if trackStateFromSource were called in parallel. The alternative would be to use a
    // ReadWriteLock where trackRecord would acquire the read and trackStateFromSource would
    // acquire the write.
    val emittedStatsToUpdate = emittedStats
    with(emittedStatsToUpdate) {
      remittedRecordsCount.incrementAndGet()
      emittedBytesCount.addAndGet(estimatedBytesSize)
    }

    // Update the global stream stats
    with(streamStats) {
      emittedRecordsCount.incrementAndGet()
      emittedBytesCount.addAndGet(estimatedBytesSize)
    }

    if (logsForStripeChecksumDebugging && !alreadyLogged && stateIds.size > 0) {
      logger.info {
        "Received records for the stream ${nameNamespacePair.namespace}:${nameNamespacePair.name}, " +
          " after receiving a state message"
      }
      alreadyLogged = true
    }
  }

  /**
   * Bookkeeping for when a state is read from the source.
   *
   * Reading a state from the source means that we need to stage our current counters. All the
   * in-flight records since the last state are associated to this state and will be kept aside until
   * we see that same state back from the destination. We should also recreate a new set of counters
   * to keep on tracking incoming messages.
   */
  fun trackStateFromSource(stateMessage: AirbyteStateMessage) {
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
      metricClient.count(OssMetricsRegistry.STATE_ERROR_COLLISION_FROM_SOURCE, 1)
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
    val currentTime = LocalDateTime.now()
    streamStats.destinationStateCount.incrementAndGet()

    if (streamStats.unreliableStateOperations.get()) {
      // State collision previously detected, we skip all operations that involve state tracking.
      return
    }

    val stateId: Int = stateMessage.getStateIdForStatsTracking()
    if (!stateIds.contains(stateId)) {
      metricClient.count(OssMetricsRegistry.STATE_ERROR_UNKNOWN_FROM_DESTINATION, 1)
      logger.warn {
        "Unexpected state from destination for stream ${nameNamespacePair.namespace}:${nameNamespacePair.name}, " +
          "$stateId not found in the stored stateIds"
      }
      return
    } else if (stagedStatsList.isEmpty()) {
      metricClient.count(OssMetricsRegistry.STATE_ERROR_UNKNOWN_FROM_DESTINATION, 1)
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

      // Increment committed stats as we are un-staging stats
      streamStats.committedBytesCount.addAndGet(stagedStats.emittedStatsCounters.emittedBytesCount.get())
      streamStats.committedRecordsCount.addAndGet(stagedStats.emittedStatsCounters.remittedRecordsCount.get())

      if (stagedStats.stateId == stateId) {
        break
      }
    }

    if (logsForStripeChecksumDebugging) {
      logger.info {
        "Received state message back from destination for the stream , " +
          "${nameNamespacePair.namespace}:${nameNamespacePair.name}, " +
          "committed record count is ${streamStats.committedRecordsCount} , total records at this point is ${streamStats.emittedRecordsCount} "
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

  /**
   * Bookkeeping for when we see an estimate message.
   */
  fun trackEstimates(msg: AirbyteEstimateTraceMessage): Unit =
    with(streamStats) {
      estimatedBytesCount.set(msg.byteEstimate)
      estimatedRecordsCount.set(msg.rowEstimate)
    }

  fun getTrackedEmittedRecordsSinceLastStateMessage(): Long {
    return previousEmittedStats.remittedRecordsCount.get()
  }

  fun getTrackedCommittedRecordsSinceLastStateMessage(stateMessage: AirbyteStateMessage): Long {
    val stateId = stateMessage.getStateIdForStatsTracking()
    val stagedStats: StagedStats? = stagedStatsList.find { it.stateId == stateId }
    if (stagedStats == null) {
      logger.warn { "Could not find the state message with id $stateId in the stagedStatsList" }
    }
    return stagedStats?.emittedStatsCounters?.remittedRecordsCount?.get() ?: 0
  }

  fun areStreamStatsReliable(): Boolean {
    return !streamStats.unreliableStateOperations.get()
  }
}

fun AirbyteStateMessage.getStateHashCode(hashFunction: HashFunction): Int =
  when (type) {
    AirbyteStateMessage.AirbyteStateType.GLOBAL -> hashFunction.hashBytes(Jsons.serialize(global).toByteArray()).hashCode()
    AirbyteStateMessage.AirbyteStateType.STREAM -> hashFunction.hashBytes(Jsons.serialize(stream.streamState).toByteArray()).hashCode()
    // state type is legacy
    else -> hashFunction.hashBytes(Jsons.serialize(data).toByteArray()).hashCode()
  }

fun AirbyteStateMessage.getStateIdForStatsTracking(): Int = StateWithId.getIdFromStateMessage(this)

private fun updateMean(
  previousMean: Double,
  previousCount: Long,
  newDataPoint: Double,
): Double {
  val newCount = previousCount + 1
  return ((previousMean * previousCount) + newDataPoint) / newCount
}
