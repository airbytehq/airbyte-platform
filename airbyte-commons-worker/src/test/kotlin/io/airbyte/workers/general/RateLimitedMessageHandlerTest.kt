package io.airbyte.workers.general

import io.airbyte.protocol.models.AirbyteMessage
import io.airbyte.protocol.models.AirbyteRecordMessage
import io.airbyte.protocol.models.AirbyteStreamStatusRateLimitedReason
import io.airbyte.protocol.models.AirbyteStreamStatusReason
import io.airbyte.protocol.models.AirbyteStreamStatusTraceMessage
import io.airbyte.protocol.models.AirbyteTraceMessage
import io.airbyte.protocol.models.StreamDescriptor
import io.airbyte.workers.context.ReplicationContext
import io.airbyte.workers.helper.AirbyteMessageDataExtractor
import io.airbyte.workers.internal.bookkeeping.events.ReplicationAirbyteMessageEventPublishingHelper
import io.mockk.Runs
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class RateLimitedMessageHandlerTest {
  private lateinit var rateLimitedMessageHandler: RateLimitedMessageHandler
  private lateinit var airbyteMessageDataExtractor: AirbyteMessageDataExtractor
  private lateinit var replicationAirbyteMessageEventPublishingHelper: ReplicationAirbyteMessageEventPublishingHelper

  @BeforeEach
  fun setUp() {
    airbyteMessageDataExtractor = mockk()
    replicationAirbyteMessageEventPublishingHelper = mockk()
    rateLimitedMessageHandler = spyk(RateLimitedMessageHandler(airbyteMessageDataExtractor, replicationAirbyteMessageEventPublishingHelper, true))
  }

  @AfterEach
  fun tearDown() {
    clearAllMocks()
  }

  @Test
  fun `test isStreamStatusRateLimitedMessage with AirbyteMessage - not TRACE type`() {
    val msg = mockk<AirbyteMessage>()
    every { msg.type } returns AirbyteMessage.Type.LOG
    assertFalse(RateLimitedMessageHandler.isStreamStatusRateLimitedMessage(msg))
  }

  @Test
  fun `test isStreamStatusRateLimitedMessage with AirbyteMessage - not STREAM_STATUS type`() {
    val traceMessage = mockk<AirbyteTraceMessage>()
    every { traceMessage.type } returns AirbyteTraceMessage.Type.ERROR
    val msg = mockk<AirbyteMessage>()
    every { msg.type } returns AirbyteMessage.Type.TRACE
    every { msg.trace } returns traceMessage
    assertFalse(RateLimitedMessageHandler.isStreamStatusRateLimitedMessage(msg))
  }

  @Test
  fun `test isStreamStatusRateLimitedMessage with AirbyteMessage - STREAM_STATUS and rate limited`() {
    val reason = mockk<AirbyteStreamStatusReason>()
    every { reason.type } returns AirbyteStreamStatusReason.AirbyteStreamStatusReasonType.RATE_LIMITED

    val streamStatusTraceMessage = mockk<AirbyteStreamStatusTraceMessage>()
    every { streamStatusTraceMessage.status } returns AirbyteStreamStatusTraceMessage.AirbyteStreamStatus.RUNNING
    every { streamStatusTraceMessage.reasons } returns listOf(reason)

    val traceMessage = mockk<AirbyteTraceMessage>()
    every { traceMessage.type } returns AirbyteTraceMessage.Type.STREAM_STATUS
    every { traceMessage.streamStatus } returns streamStatusTraceMessage

    val msg = mockk<AirbyteMessage>()
    every { msg.type } returns AirbyteMessage.Type.TRACE
    every { msg.trace } returns traceMessage

    assertTrue(RateLimitedMessageHandler.isStreamStatusRateLimitedMessage(msg))
  }

  @Test
  fun `test isStreamStatusRateLimitedMessage with AirbyteStreamStatusTraceMessage - no reasons`() {
    val msg = mockk<AirbyteStreamStatusTraceMessage>()
    every { msg.reasons } returns null
    assertFalse(RateLimitedMessageHandler.isStreamStatusRateLimitedMessage(msg))
  }

  @Test
  fun `test isStreamStatusRateLimitedMessage with AirbyteStreamStatusTraceMessage - multiple reasons`() {
    val reason1 = mockk<AirbyteStreamStatusReason>()
    val reason2 = mockk<AirbyteStreamStatusReason>()
    val msg = mockk<AirbyteStreamStatusTraceMessage>()
    every { msg.reasons } returns listOf(reason1, reason2)
    assertFalse(RateLimitedMessageHandler.isStreamStatusRateLimitedMessage(msg))
  }

  @Test
  fun `test isStreamStatusRateLimitedMessage with AirbyteStreamStatusTraceMessage - correct reason`() {
    val reason = mockk<AirbyteStreamStatusReason>()
    every { reason.type } returns AirbyteStreamStatusReason.AirbyteStreamStatusReasonType.RATE_LIMITED
    val msg = mockk<AirbyteStreamStatusTraceMessage>()
    every { msg.reasons } returns listOf(reason)
    assertTrue(RateLimitedMessageHandler.isStreamStatusRateLimitedMessage(msg))
  }

  @Test
  fun `test extractQuotaResetValue - no reasons`() {
    val msg = mockk<AirbyteStreamStatusTraceMessage>()
    every { msg.reasons } returns null
    assertNull(RateLimitedMessageHandler.extractQuotaResetValue(msg))
  }

  @Test
  fun `test extractQuotaResetValue - multiple reasons`() {
    val reason1 = mockk<AirbyteStreamStatusReason>()
    val reason2 = mockk<AirbyteStreamStatusReason>()
    val msg = mockk<AirbyteStreamStatusTraceMessage>()
    every { msg.reasons } returns listOf(reason1, reason2)
    assertNull(RateLimitedMessageHandler.extractQuotaResetValue(msg))
  }

  @Test
  fun `test extractQuotaResetValue - correct reason with quota reset`() {
    val rateLimited = mockk<AirbyteStreamStatusRateLimitedReason>()
    every { rateLimited.quotaReset } returns 123L
    val reason = mockk<AirbyteStreamStatusReason>()
    every { reason.type } returns AirbyteStreamStatusReason.AirbyteStreamStatusReasonType.RATE_LIMITED
    every { reason.rateLimited } returns rateLimited
    val msg = mockk<AirbyteStreamStatusTraceMessage>()
    every { msg.reasons } returns listOf(reason)
    assertEquals(123L, RateLimitedMessageHandler.extractQuotaResetValue(msg))
  }

  @Test
  fun `test extractQuotaResetValue - correct reason without quota reset`() {
    val rateLimited = mockk<AirbyteStreamStatusRateLimitedReason>()
    every { rateLimited.quotaReset } returns null
    val reason = mockk<AirbyteStreamStatusReason>()
    every { reason.type } returns AirbyteStreamStatusReason.AirbyteStreamStatusReasonType.RATE_LIMITED
    every { reason.rateLimited } returns rateLimited
    val msg = mockk<AirbyteStreamStatusTraceMessage>()
    every { msg.reasons } returns listOf(reason)
    assertNull(RateLimitedMessageHandler.extractQuotaResetValue(msg))
  }

  @Test
  fun `test clear method`() {
    val streamName = mockk<StreamDescriptor>()
    val timeValue = 123L
    rateLimitedMessageHandler.streamsInRateLimitedState.put(streamName, timeValue)
    rateLimitedMessageHandler.clear()
    assertTrue(rateLimitedMessageHandler.streamsInRateLimitedState.isEmpty())
  }

  @Test
  fun `test moveStreamOutOfRateLimitedStateIfApplicable when stream is in rate limited state`() {
    val streamDescriptor = mockk<StreamDescriptor>()
    val sourceRawMessage = mockk<AirbyteMessage>()
    val context = mockk<ReplicationContext>()
    val record = mockk<AirbyteRecordMessage>()

    every { sourceRawMessage.type } returns AirbyteMessage.Type.RECORD
    every { sourceRawMessage.record } returns record
    every { record.emittedAt } returns 123L
    every { airbyteMessageDataExtractor.getStreamFromMessage(sourceRawMessage) } returns streamDescriptor
    every { rateLimitedMessageHandler.markStreamAsRunning(streamDescriptor, context, 123L) } just Runs

    rateLimitedMessageHandler.streamsInRateLimitedState.put(streamDescriptor, 120L)

    rateLimitedMessageHandler.moveStreamOutOfRateLimitedStateIfApplicable(sourceRawMessage, context)

    verify { rateLimitedMessageHandler.markStreamAsRunning(streamDescriptor, context, 123L) }
    assertFalse(rateLimitedMessageHandler.streamsInRateLimitedState.containsKey(streamDescriptor))
  }

  @Test
  fun `test moveStreamOutOfRateLimitedStateIfApplicable when stream is not in rate limited state`() {
    val streamDescriptor = mockk<StreamDescriptor>()
    val sourceRawMessage = mockk<AirbyteMessage>()
    val context = mockk<ReplicationContext>()

    every { sourceRawMessage.type } returns AirbyteMessage.Type.RECORD
    every { airbyteMessageDataExtractor.getStreamFromMessage(sourceRawMessage) } returns streamDescriptor

    rateLimitedMessageHandler.moveStreamOutOfRateLimitedStateIfApplicable(sourceRawMessage, context)

    verify(exactly = 0) { rateLimitedMessageHandler.markStreamAsRunning(any(), any(), any()) }
  }

  @Test
  fun `test moveStreamToRateLimitedStateIfApplicable when stream is rate limited`() {
    val streamDescriptor = mockk<StreamDescriptor>()

    val reason = mockk<AirbyteStreamStatusReason>()
    every { reason.type } returns AirbyteStreamStatusReason.AirbyteStreamStatusReasonType.RATE_LIMITED

    val streamStatusTraceMessage = mockk<AirbyteStreamStatusTraceMessage>()
    every { streamStatusTraceMessage.status } returns AirbyteStreamStatusTraceMessage.AirbyteStreamStatus.RUNNING
    every { streamStatusTraceMessage.reasons } returns listOf(reason)

    val traceMessage = mockk<AirbyteTraceMessage>()
    every { traceMessage.type } returns AirbyteTraceMessage.Type.STREAM_STATUS
    every { traceMessage.streamStatus } returns streamStatusTraceMessage
    every { traceMessage.emittedAt } returns 123.0

    val msg = mockk<AirbyteMessage>()
    every { msg.type } returns AirbyteMessage.Type.TRACE
    every { msg.trace } returns traceMessage

    every { airbyteMessageDataExtractor.getStreamFromMessage(msg) } returns streamDescriptor

    rateLimitedMessageHandler.moveStreamToRateLimitedStateIfApplicable(msg)

    assertTrue(rateLimitedMessageHandler.streamsInRateLimitedState.containsKey(streamDescriptor))
  }

  @Test
  fun `test moveStreamToRateLimitedStateIfApplicable when stream is not rate limited`() {
    val sourceRawMessage = mockk<AirbyteMessage>()

    every { sourceRawMessage.type } returns AirbyteMessage.Type.RECORD

    rateLimitedMessageHandler.moveStreamToRateLimitedStateIfApplicable(sourceRawMessage)

    verify(exactly = 0) { airbyteMessageDataExtractor.getStreamFromMessage(any()) }
  }
}
