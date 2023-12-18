package io.airbyte.workers.internal

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import io.airbyte.analytics.TrackingClient
import io.airbyte.commons.json.Jsons
import io.airbyte.protocol.models.AirbyteMessage
import io.airbyte.protocol.models.AirbyteTraceMessage
import io.airbyte.workers.context.ReplicationContext
import io.airbyte.workers.internal.bookkeeping.AirbyteMessageOrigin
import jakarta.inject.Singleton
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger

const val MAX_ANALYTICS_MESSAGES_PER_SYNC = 1000
const val MAX_ANALYTICS_MESSAGES_PER_BATCH = 100

@Singleton
class AnalyticsMessageTracker(private val trackingClient: TrackingClient) {
  var ctx: ReplicationContext? = null
  private val messages = Collections.synchronizedList(mutableListOf<JsonNode>())
  private val totalNumberOfMessages = AtomicInteger(0)

  fun addMessage(
    msg: AirbyteMessage,
    origin: AirbyteMessageOrigin,
  ) {
    if (msg.type != AirbyteMessage.Type.TRACE || msg.trace.type != AirbyteTraceMessage.Type.ANALYTICS) {
      return
    }
    if (totalNumberOfMessages.incrementAndGet() > MAX_ANALYTICS_MESSAGES_PER_SYNC) {
      return
    }

    messages.add(
      Jsons.jsonNode(
        mapOf(
          "origin" to origin.toString(),
          "type" to msg.trace.analytics.type,
          "value" to msg.trace.analytics.value,
          "timestamp" to System.currentTimeMillis(),
        ),
      ),
    )
    if (messages.size >= MAX_ANALYTICS_MESSAGES_PER_BATCH) {
      flush()
    }
  }

  private fun generateAnalyticsMetadata(currentMessages: List<JsonNode>): Map<String?, Any?>? {
    val context = requireNotNull(ctx)
    val jsonList: ArrayNode = Jsons.arrayNode()
    jsonList.addAll(currentMessages)

    return mapOf(
      "analytics_messages" to jsonList.toString(),
      "workspace_id" to context.workspaceId,
      "connection_id" to context.connectionId,
      "job_id" to context.jobId,
      "attempt" to context.attempt,
    )
  }

  @Synchronized
  private fun getCurrentMessages(): List<JsonNode> {
    val currentMessages = messages.toList()
    messages.clear()

    return currentMessages
  }

  fun flush() {
    val currentMessages = getCurrentMessages()
    if (currentMessages.isNotEmpty()) {
      val context = requireNotNull(ctx)
      trackingClient.track(context.workspaceId, "analytics_messages", generateAnalyticsMetadata(currentMessages))
    }
  }
}
