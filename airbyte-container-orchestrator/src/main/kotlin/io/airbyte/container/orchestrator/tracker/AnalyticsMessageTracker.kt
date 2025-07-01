/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.tracker

import io.airbyte.analytics.TrackingClient
import io.airbyte.config.ScopeType
import io.airbyte.container.orchestrator.bookkeeping.AirbyteMessageOrigin
import io.airbyte.container.orchestrator.worker.context.ReplicationContext
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteTraceMessage
import jakarta.inject.Singleton
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger

const val MAX_ANALYTICS_MESSAGES_PER_SYNC = 1000
const val MAX_ANALYTICS_MESSAGES_PER_BATCH = 100

@Singleton
class AnalyticsMessageTracker(
  private val trackingClient: TrackingClient,
) {
  var ctx: ReplicationContext? = null
  private val messages = Collections.synchronizedList(mutableListOf<Map<String, Any>>())
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
      mapOf(
        "origin" to origin.toString(),
        "type" to msg.trace.analytics.type,
        "value" to msg.trace.analytics.value,
        "timestamp" to System.currentTimeMillis(),
      ),
    )
    if (messages.size >= MAX_ANALYTICS_MESSAGES_PER_BATCH) {
      flush()
    }
  }

  private fun generateAnalyticsMetadata(currentMessages: List<Map<String, Any>>): Map<String, Any?> =
    requireNotNull(ctx).let { context ->
      mapOf(
        "analytics_messages" to currentMessages,
        "workspace_id" to context.workspaceId,
        "connection_id" to context.connectionId,
        "job_id" to context.jobId,
        "attempt" to context.attempt,
      )
    }

  @Synchronized
  private fun getCurrentMessages(): List<Map<String, Any>> {
    val currentMessages = messages.toList()
    messages.clear()

    return currentMessages
  }

  fun flush() {
    val currentMessages = getCurrentMessages()
    if (currentMessages.isNotEmpty()) {
      val context = requireNotNull(ctx)
      trackingClient.track(context.workspaceId, ScopeType.WORKSPACE, "analytics_messages", generateAnalyticsMetadata(currentMessages))
    }
  }
}
