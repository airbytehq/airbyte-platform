/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.tracker

import io.airbyte.analytics.TrackingClient
import io.airbyte.config.ScopeType
import io.airbyte.container.orchestrator.bookkeeping.AirbyteMessageOrigin
import io.airbyte.container.orchestrator.worker.context.ReplicationContext
import io.airbyte.protocol.models.v0.AirbyteAnalyticsTraceMessage
import io.airbyte.protocol.models.v0.AirbyteLogMessage
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteTraceMessage
import io.airbyte.workers.testutils.TestConfigHelpers
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

class AnalyticsMessageTrackerTest {
  private lateinit var trackingClient: TrackingClient
  private lateinit var analyticsMessageTracker: AnalyticsMessageTracker
  private lateinit var ctx: ReplicationContext
  private val sourceDefinitionId = UUID.randomUUID()
  private val destinationDefinitionId = UUID.randomUUID()

  @BeforeEach
  fun setUp() {
    trackingClient = mockk()
    every { trackingClient.track(any(), any(), any(), any()) } returns Unit
    analyticsMessageTracker = AnalyticsMessageTracker(trackingClient)
    ctx =
      ReplicationContext(
        false,
        UUID.randomUUID(),
        UUID.randomUUID(),
        UUID.randomUUID(),
        1,
        1,
        UUID.randomUUID(),
        TestConfigHelpers.SOURCE_IMAGE,
        TestConfigHelpers.DESTINATION_IMAGE,
        sourceDefinitionId,
        destinationDefinitionId,
      )
    analyticsMessageTracker.ctx = ctx
  }

  @Test
  fun `test addMessage and flush is called`() {
    val message = createAnalyticsAirbyteMessage()

    repeat(MAX_ANALYTICS_MESSAGES_PER_BATCH + 1) {
      analyticsMessageTracker.addMessage(message, AirbyteMessageOrigin.SOURCE)
    }

    verify(exactly = 1) { trackingClient.track(any(), ScopeType.WORKSPACE, "analytics_messages", any()) }
  }

  @Test
  fun `test ignoring non-analytics and non-trace messages`() {
    val nonAnalyticsMessage = createNonAnalyticsAirbyteMessage()
    analyticsMessageTracker.addMessage(nonAnalyticsMessage, AirbyteMessageOrigin.SOURCE)

    verify(exactly = 0) { trackingClient.track(any(), any(), any(), any()) }
  }

  @Test
  fun `test max message limit`() {
    val message = createAnalyticsAirbyteMessage()
    repeat(MAX_ANALYTICS_MESSAGES_PER_SYNC + MAX_ANALYTICS_MESSAGES_PER_BATCH + 1) {
      analyticsMessageTracker.addMessage(message, AirbyteMessageOrigin.SOURCE)
    }

    // never track more batches than what would be required to get to max analytics messages, even if more are added
    verify(
      exactly = MAX_ANALYTICS_MESSAGES_PER_SYNC / MAX_ANALYTICS_MESSAGES_PER_BATCH,
    ) { trackingClient.track(any(), ScopeType.WORKSPACE, "analytics_messages", any()) }
  }

  @Test
  fun `test flush with empty analytics message array`() {
    val message = createAnalyticsAirbyteMessage()
    val nonAnalyticsMessage = createNonAnalyticsAirbyteMessage()

    analyticsMessageTracker.addMessage(nonAnalyticsMessage, AirbyteMessageOrigin.SOURCE)

    analyticsMessageTracker.flush()

    verify(exactly = 0) { trackingClient.track(any(), any(), any(), any()) }

    analyticsMessageTracker.addMessage(message, AirbyteMessageOrigin.SOURCE)

    analyticsMessageTracker.flush()

    verify(exactly = 1) { trackingClient.track(any(), any(), any(), any()) }
  }

  @Test
  fun `test flush with non-empty messages`() {
    val message = createAnalyticsAirbyteMessage()
    analyticsMessageTracker.addMessage(message, AirbyteMessageOrigin.SOURCE)

    verify(exactly = 0) { trackingClient.track(any(), any(), any(), any()) }

    analyticsMessageTracker.flush()

    verify(exactly = 1) { trackingClient.track(any(), any(), any(), any()) }
  }

  @Test
  @Suppress("UNCHECKED_CAST")
  fun `test payload of track method`() {
    val sourceMessage = createAnalyticsAirbyteMessage()
    val destinationMessage = createAnalyticsAirbyteMessage()

    analyticsMessageTracker.addMessage(sourceMessage, AirbyteMessageOrigin.SOURCE)
    analyticsMessageTracker.addMessage(destinationMessage, AirbyteMessageOrigin.DESTINATION)
    analyticsMessageTracker.flush()

    // Capture the argument passed to track
    val payloadSlot = slot<Map<String, Any?>>()
    verify(exactly = 1) { trackingClient.track(any(), ScopeType.WORKSPACE, any(), capture(payloadSlot)) }

    // Extract and assert the captured payload
    val capturedPayload = payloadSlot.captured

    val analyticsMessages = capturedPayload["analytics_messages"] as List<Map<String, Any>>

    assertEquals(2, analyticsMessages.size)

    // Assuming the order is not guaranteed, you can iterate and check conditions
    analyticsMessages.forEach { analyticsMessage: Map<String, Any> ->
      val origin = analyticsMessage["origin"]
      val timestamp = analyticsMessage["timestamp"]
      val value = analyticsMessage["value"]
      val type = analyticsMessage["type"]

      assertTrue(origin == "SOURCE" || origin == "DESTINATION")
      assertNotNull(timestamp) // Or any specific condition you want to check for timestamp
      assertEquals("val", value)
      assertEquals("key", type)
    }

    assertEquals(capturedPayload["workspace_id"], ctx.workspaceId)
    assertEquals(capturedPayload["connection_id"], ctx.connectionId)
    assertEquals(capturedPayload["job_id"], ctx.jobId)
    assertEquals(capturedPayload["attempt"], ctx.attempt)
  }

  private fun createAnalyticsAirbyteMessage(): AirbyteMessage {
    val analyticsTraceMessage = AirbyteAnalyticsTraceMessage().withType("key").withValue("val")
    val traceMessage = AirbyteTraceMessage().withType(AirbyteTraceMessage.Type.ANALYTICS).withAnalytics(analyticsTraceMessage)
    return AirbyteMessage().withType(AirbyteMessage.Type.TRACE).withTrace(traceMessage)
  }

  private fun createNonAnalyticsAirbyteMessage(): AirbyteMessage {
    val logMessage = AirbyteLogMessage().withMessage("test")
    return AirbyteMessage().withType(AirbyteMessage.Type.LOG).withLog(logMessage)
  }
}
