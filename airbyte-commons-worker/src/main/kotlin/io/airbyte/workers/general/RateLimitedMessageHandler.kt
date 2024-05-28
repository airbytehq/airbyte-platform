package io.airbyte.workers.general

import io.airbyte.protocol.models.AirbyteMessage
import io.airbyte.protocol.models.AirbyteStreamStatusReason
import io.airbyte.protocol.models.AirbyteStreamStatusTraceMessage
import io.airbyte.protocol.models.AirbyteTraceMessage
import io.airbyte.protocol.models.StreamDescriptor
import io.airbyte.workers.context.ReplicationContext
import io.airbyte.workers.helper.AirbyteMessageDataExtractor
import io.airbyte.workers.internal.bookkeeping.AirbyteMessageOrigin
import io.airbyte.workers.internal.bookkeeping.events.ReplicationAirbyteMessageEventPublishingHelper
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

class RateLimitedMessageHandler(
  private val airbyteMessageDataExtractor: AirbyteMessageDataExtractor,
  private val replicationAirbyteMessageEventPublishingHelper: ReplicationAirbyteMessageEventPublishingHelper,
  private val processRateLimitedMessage: Boolean,
) {
  internal val streamsInRateLimitedState = ConcurrentHashMap<StreamDescriptor, Long>()

  fun clear() = run { streamsInRateLimitedState.clear() }

  fun moveStreamOutOfRateLimitedStateIfApplicable(
    sourceRawMessage: AirbyteMessage,
    context: ReplicationContext,
  ) {
    if (!processRateLimitedMessage) {
      return
    }
    if (!streamsInRateLimitedState.isEmpty() && sourceRawMessage.type == AirbyteMessage.Type.RECORD) {
      airbyteMessageDataExtractor.getStreamFromMessage(sourceRawMessage)?.let {
        if (streamsInRateLimitedState.containsKey(it)) {
          val timeOfRateLimitedMessage: Long = streamsInRateLimitedState.get(it) ?: Instant.EPOCH.toEpochMilli()
          val timeFromRecord = extractEmittedAt(sourceRawMessage) ?: System.currentTimeMillis()
          val transitionTime = if (timeFromRecord > timeOfRateLimitedMessage) timeFromRecord else System.currentTimeMillis()
          markStreamAsRunning(it, context, transitionTime)
          streamsInRateLimitedState.remove(it)
        }
      }
    }
  }

  fun moveStreamToRateLimitedStateIfApplicable(sourceRawMessage: AirbyteMessage) {
    if (!processRateLimitedMessage) {
      return
    }
    if (isStreamStatusRateLimitedMessage(sourceRawMessage)) {
      airbyteMessageDataExtractor.getStreamFromMessage(sourceRawMessage)?.let {
        streamsInRateLimitedState.put(it, extractEmittedAt(sourceRawMessage) ?: System.currentTimeMillis())
      }
    }
  }

  internal fun markStreamAsRunning(
    streamFromMessage: StreamDescriptor,
    context: ReplicationContext,
    emittedAt: Long,
  ) {
    streamFromMessage.let {
      replicationAirbyteMessageEventPublishingHelper.publishRunningStatusEvent(
        streamFromMessage,
        context,
        AirbyteMessageOrigin.SOURCE,
        emittedAt,
      )
    }
  }

  companion object {
    internal fun isStreamStatusRateLimitedMessage(msg: AirbyteMessage): Boolean =
      when {
        msg.type != AirbyteMessage.Type.TRACE -> false
        msg.trace.type != AirbyteTraceMessage.Type.STREAM_STATUS -> false
        msg.trace.streamStatus != null && isStreamStatusRateLimitedMessage(msg.trace.streamStatus) -> true
        else -> true
      }

    internal fun isStreamStatusRateLimitedMessage(msg: AirbyteStreamStatusTraceMessage): Boolean =
      when {
        msg.reasons?.size != 1 -> false
        msg.reasons[0]?.type != AirbyteStreamStatusReason.AirbyteStreamStatusReasonType.RATE_LIMITED -> false
        else -> true
      }

    internal fun extractQuotaResetValue(msg: AirbyteStreamStatusTraceMessage): Long? =
      when {
        msg.reasons?.size != 1 -> null
        msg.reasons[0]?.type != AirbyteStreamStatusReason.AirbyteStreamStatusReasonType.RATE_LIMITED -> null
        else -> msg.reasons[0]?.rateLimited?.quotaReset
      }

    internal fun extractEmittedAt(msg: AirbyteMessage): Long? =
      when {
        msg.type == AirbyteMessage.Type.RECORD -> msg.record.emittedAt
        msg.type == AirbyteMessage.Type.TRACE -> msg.trace.emittedAt.toLong()
        else -> null
      }
  }
}
