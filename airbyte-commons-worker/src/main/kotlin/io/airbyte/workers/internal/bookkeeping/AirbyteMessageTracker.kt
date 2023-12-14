package io.airbyte.workers.internal.bookkeeping

import io.airbyte.commons.features.FeatureFlags
import io.airbyte.commons.json.Jsons
import io.airbyte.config.FailureReason
import io.airbyte.protocol.models.AirbyteAnalyticsTraceMessage
import io.airbyte.protocol.models.AirbyteMessage
import io.airbyte.protocol.models.AirbyteTraceMessage
import io.airbyte.workers.helper.FailureHelper
import io.airbyte.workers.internal.stateaggregator.DefaultStateAggregator
import io.airbyte.workers.internal.stateaggregator.StateAggregator
import io.github.oshai.kotlinlogging.KotlinLogging

private val logger = KotlinLogging.logger {}

/**
 * This class is responsible for stats and metadata tracking surrounding [AirbyteRecordMessage].
 *
 * It is not intended to perform meaningful operations - transforming, mutating, triggering
 * downstream actions etc. - on specific messages.
 */
class AirbyteMessageTracker(
  val syncStatsTracker: SyncStatsTracker,
  featureFlags: FeatureFlags,
  private val sourceDockerImage: String,
  private val destinationDockerImage: String,
  private val trackCommittedStatsWhenUsingGlobalState: Boolean,
) {
  private val dstErrorTraceMsgs = ArrayList<AirbyteTraceMessage>()
  private val srcErrorTraceMsgs = ArrayList<AirbyteTraceMessage>()
  private val logConnectorMsgs: Boolean = featureFlags.logConnectorMessages()
  private val stateAggregator: StateAggregator = DefaultStateAggregator()

  /**
   * Accepts an AirbyteMessage emitted from a source and tracks any metadata about it that is required
   * by the Platform.
   *
   * @param msg message to derive metadata from.
   */
  fun acceptFromSource(msg: AirbyteMessage) {
    logMsgAsJson("source", msg)

    when (msg.type) {
      AirbyteMessage.Type.TRACE -> handleEmittedTrace(msg.trace, AirbyteMessageOrigin.SOURCE)
      AirbyteMessage.Type.RECORD -> syncStatsTracker.updateStats(msg.record)
      AirbyteMessage.Type.STATE -> syncStatsTracker.updateSourceStatesStats(msg.state, trackCommittedStatsWhenUsingGlobalState)
      AirbyteMessage.Type.CONTROL -> logger.debug { "Control message not currently tracked." }
      else -> logger.warn { "Invalid message type for message: $msg" }
    }
  }

  /**
   * Accepts an AirbyteMessage emitted from a destination and tracks any metadata about it that is
   * required by the Platform.
   *
   * @param msg message to derive metadata from.
   */
  fun acceptFromDestination(msg: AirbyteMessage) {
    logMsgAsJson("destination", msg)

    when (msg.type) {
      AirbyteMessage.Type.TRACE -> handleEmittedTrace(msg.trace, AirbyteMessageOrigin.DESTINATION)
      AirbyteMessage.Type.STATE ->
        msg.state?.let {
          stateAggregator.ingest(it)
          syncStatsTracker.updateDestinationStateStats(it, trackCommittedStatsWhenUsingGlobalState)
        }
      AirbyteMessage.Type.CONTROL -> logger.debug { "Control message not currently tracked." }
      else -> logger.warn { " Invalid message type for message: $msg" }
    }
  }

  fun errorTraceMessageFailure(
    jobId: Long,
    attempt: Int,
  ): FailureReason? {
    val srcMsg = srcErrorTraceMsgs.firstOrNull()
    val dstMsg = dstErrorTraceMsgs.firstOrNull()

    return when {
      srcMsg == null && dstMsg == null -> null
      srcMsg != null && dstMsg == null -> FailureHelper.sourceFailure(srcMsg, jobId, attempt)
      srcMsg == null && dstMsg != null -> FailureHelper.destinationFailure(dstMsg, jobId, attempt)
      srcMsg != null && dstMsg != null && srcMsg.emittedAt <= dstMsg.emittedAt -> FailureHelper.sourceFailure(srcMsg, jobId, attempt)
      else -> FailureHelper.destinationFailure(dstMsg, jobId, attempt)
    }
  }

  /**
   * When a connector emits a trace message, check the type and call the correct function. If it is an
   * error trace message, add it to the list of errorTraceMessages for the connector type
   */
  private fun handleEmittedTrace(
    msg: AirbyteTraceMessage,
    origin: AirbyteMessageOrigin,
  ): Unit =
    when (msg.type) {
      AirbyteTraceMessage.Type.ESTIMATE -> syncStatsTracker.updateEstimates(msg.estimate)
      AirbyteTraceMessage.Type.ERROR -> handleEmittedTraceError(msg, origin)
      AirbyteTraceMessage.Type.ANALYTICS -> handleEmittedAnalyticsMessage(msg.analytics, origin)
      AirbyteTraceMessage.Type.STREAM_STATUS -> logger.debug { "Stream status trace message not handled by message tracker: $msg" }
      else -> logger.warn { "Invalid message type for trace message: $msg" }
    }

  private fun handleEmittedTraceError(
    msg: AirbyteTraceMessage,
    origin: AirbyteMessageOrigin,
  ) {
    when (origin) {
      AirbyteMessageOrigin.SOURCE -> srcErrorTraceMsgs.add(msg)
      AirbyteMessageOrigin.DESTINATION -> dstErrorTraceMsgs.add(msg)
      AirbyteMessageOrigin.INTERNAL -> logger.debug { "internal messages are not tracked. " }
    }
  }

  /**
   * Log analytics message - logs can be searched for certain events to analyze.
   * This will be replaced by logic to collect the messages and attach them to the attempt summary in a subsequent PR.
   */
  private fun handleEmittedAnalyticsMessage(
    msg: AirbyteAnalyticsTraceMessage,
    origin: AirbyteMessageOrigin,
  ) {
    val dockerImage = if (origin == AirbyteMessageOrigin.SOURCE) sourceDockerImage else destinationDockerImage
    logger.info { "$origin analytics [$dockerImage] | Type: ${msg.type} | Value: ${msg.value}" }
  }

  private fun logMsgAsJson(
    caller: String,
    msg: AirbyteMessage,
  ): Unit =
    when (logConnectorMsgs) {
      true -> logger.info { "$caller message | ${Jsons.serialize(msg)}" }
      else -> Unit
    }
}
