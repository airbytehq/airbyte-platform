/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.util

import io.airbyte.protocol.models.v0.AirbyteControlMessage
import io.airbyte.protocol.models.v0.AirbyteErrorTraceMessage
import io.airbyte.protocol.models.v0.AirbyteEstimateTraceMessage
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteRecordMessage
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStreamState
import io.airbyte.protocol.models.v0.AirbyteStreamStatusTraceMessage
import io.airbyte.protocol.models.v0.AirbyteTraceMessage
import io.airbyte.protocol.models.v0.StreamDescriptor
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private const val NAME = "name"
private const val NAMESPACE = "namespace"

/**
 * Test suite for the [AirbyteMessageDataExtractor] class.
 */
internal class AirbyteMessageDataExtractorTest {
  private lateinit var defaultValue: StreamDescriptor
  private lateinit var testStreamDescriptor: StreamDescriptor
  private lateinit var airbyteMessageDataExtractor: AirbyteMessageDataExtractor

  @BeforeEach
  fun setup() {
    defaultValue = StreamDescriptor().withName("default").withNamespace("default")
    testStreamDescriptor = StreamDescriptor().withName(NAME).withNamespace(NAMESPACE)
    airbyteMessageDataExtractor = AirbyteMessageDataExtractor()
  }

  @Test
  fun testExtractStreamDescriptorControlMessage() {
    val airbyteControlMessage = mockk<AirbyteControlMessage>()
    val airbyteMessage = AirbyteMessage().withControl(airbyteControlMessage).withType(AirbyteMessage.Type.CONTROL)
    val extracted = airbyteMessageDataExtractor.extractStreamDescriptor(airbyteMessage, defaultValue)
    assertEquals(defaultValue, extracted)
  }

  @Test
  fun testExtractStreamDescriptorRecordMessage() {
    val airbyteRecordMessage =
      mockk<AirbyteRecordMessage> {
        every { namespace } returns testStreamDescriptor.namespace
        every { stream } returns testStreamDescriptor.name
      }

    val airbyteMessage = AirbyteMessage().withRecord(airbyteRecordMessage).withType(AirbyteMessage.Type.RECORD)
    val extracted = airbyteMessageDataExtractor.extractStreamDescriptor(airbyteMessage!!, defaultValue)
    assertEquals(testStreamDescriptor, extracted)
  }

  @Test
  fun testExtractStreamDescriptorStateMessage() {
    val airbyteStreamState =
      mockk<AirbyteStreamState> {
        every { streamDescriptor } returns testStreamDescriptor
      }
    val airbyteStateMessage =
      mockk<AirbyteStateMessage> {
        every { stream } returns airbyteStreamState
      }

    val airbyteMessage = AirbyteMessage().withState(airbyteStateMessage).withType(AirbyteMessage.Type.STATE)
    val extracted = airbyteMessageDataExtractor.extractStreamDescriptor(airbyteMessage!!, defaultValue)
    assertEquals(testStreamDescriptor, extracted)
  }

  @Test
  fun testExtractStreamDescriptorStateMessageWithoutStream() {
    val airbyteStateMessage =
      mockk<AirbyteStateMessage> {
        every { stream } returns null
      }

    val airbyteMessage = AirbyteMessage().withState(airbyteStateMessage).withType(AirbyteMessage.Type.STATE)
    val extracted = airbyteMessageDataExtractor.extractStreamDescriptor(airbyteMessage!!, defaultValue)
    assertEquals(defaultValue, extracted)
  }

  @Test
  fun testExtractStreamDescriptorErrorTraceMessage() {
    val airbyteErrorTraceMessage =
      mockk<AirbyteErrorTraceMessage> {
        every { streamDescriptor } returns testStreamDescriptor
      }
    val airbyteTraceMessage =
      mockk<AirbyteTraceMessage> {
        every { type } returns AirbyteTraceMessage.Type.ERROR
        every { error } returns airbyteErrorTraceMessage
      }

    val airbyteMessage = AirbyteMessage().withTrace(airbyteTraceMessage).withType(AirbyteMessage.Type.TRACE)
    val extracted = airbyteMessageDataExtractor.extractStreamDescriptor(airbyteMessage!!, defaultValue)
    assertEquals(testStreamDescriptor, extracted)
  }

  @Test
  fun testExtractStreamDescriptorEstimateTraceMessage() {
    val airbyteEstimateTraceMessage =
      mockk<AirbyteEstimateTraceMessage> {
        every { name } returns testStreamDescriptor.name
        every { namespace } returns testStreamDescriptor.namespace
      }
    val airbyteTraceMessage =
      mockk<AirbyteTraceMessage> {
        every { type } returns AirbyteTraceMessage.Type.ESTIMATE
        every { estimate } returns airbyteEstimateTraceMessage
      }

    val airbyteMessage = AirbyteMessage().withTrace(airbyteTraceMessage).withType(AirbyteMessage.Type.TRACE)
    val extracted = airbyteMessageDataExtractor.extractStreamDescriptor(airbyteMessage!!, defaultValue)
    assertEquals(testStreamDescriptor, extracted)
  }

  @Test
  fun testExtractStreamDescriptorStreamStatusTraceMessage() {
    val airbyteStreamStatusTraceMessage =
      mockk<AirbyteStreamStatusTraceMessage> {
        every { streamDescriptor } returns testStreamDescriptor
      }
    val airbyteTraceMessage =
      mockk<AirbyteTraceMessage> {
        every { type } returns AirbyteTraceMessage.Type.STREAM_STATUS
        every { streamStatus } returns airbyteStreamStatusTraceMessage
      }

    val airbyteMessage = AirbyteMessage().withTrace(airbyteTraceMessage).withType(AirbyteMessage.Type.TRACE)
    val extracted = airbyteMessageDataExtractor.extractStreamDescriptor(airbyteMessage!!, defaultValue)
    assertEquals(testStreamDescriptor, extracted)
  }
}

/**
 * Extension function that extracts the stream descriptor from the provided [AirbyteMessage], returning the provided
 * `defaultValue` value if the [AirbyteMessage] does not include a stream descriptor.
 *
 * @param airbyteMessage The [AirbyteMessage] that may contain stream information.
 * @param defaultValue The default value to return if the provided [AirbyteMessage] does not
 * contain stream information.
 * @return The [StreamDescriptor] extracted from the provided [AirbyteMessage] or the
 * provided `defaultValue` if the message does not contain stream information.
 */
fun AirbyteMessageDataExtractor.extractStreamDescriptor(
  airbyteMessage: AirbyteMessage,
  defaultValue: StreamDescriptor?,
): StreamDescriptor? {
  val extractedStreamDescriptor = this.getStreamFromMessage(airbyteMessage)
  return extractedStreamDescriptor ?: defaultValue
}
