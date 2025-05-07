/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.tracker

import io.airbyte.commons.json.Jsons
import io.airbyte.config.AirbyteStream
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.DestinationSyncMode
import io.airbyte.config.SyncMode
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteStreamStatusTraceMessage
import io.airbyte.protocol.models.v0.AirbyteTraceMessage
import io.airbyte.protocol.models.v0.StreamDescriptor
import io.airbyte.workers.internal.AirbyteMapper
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import java.time.Clock

internal class StreamStatusCompletionTrackerTest {
  private val clock: Clock = mockk()
  private val mapper: AirbyteMapper = mockk()

  private val streamStatusCompletionTracker = StreamStatusCompletionTracker(clock)

  private val catalog =
    ConfiguredAirbyteCatalog()
      .withStreams(
        listOf(
          ConfiguredAirbyteStream(
            AirbyteStream(
              name = "name1",
              jsonSchema = Jsons.emptyObject(),
              supportedSyncModes = listOf(SyncMode.INCREMENTAL),
            ),
            SyncMode.INCREMENTAL,
            DestinationSyncMode.APPEND,
          ),
          ConfiguredAirbyteStream(
            AirbyteStream(
              name = "name2",
              namespace = "namespace2",
              jsonSchema = Jsons.emptyObject(),
              supportedSyncModes = listOf(SyncMode.INCREMENTAL),
            ),
            SyncMode.INCREMENTAL,
            DestinationSyncMode.APPEND,
          ),
        ),
      )

  @BeforeEach
  fun init() {
    every { clock.millis() } returns 1
    every { mapper.mapMessage(any()) } returnsArgument 0
  }

  @Test
  fun `test that we get all the streams if the exit code is 0 and no stream status is send`() {
    streamStatusCompletionTracker.startTracking(catalog, true)
    val result = streamStatusCompletionTracker.finalize(0, mapper)

    Assertions.assertEquals(
      listOf(
        getStreamStatusCompletedMessage("name1"),
        getStreamStatusCompletedMessage("name2", "namespace2"),
      ),
      result,
    )
  }

  @Test
  fun `test that we get all the streams if the exit code is 0 and some stream status is send`() {
    streamStatusCompletionTracker.startTracking(catalog, true)
    streamStatusCompletionTracker.track(getStreamStatusCompletedMessage("name1").trace.streamStatus)
    val result = streamStatusCompletionTracker.finalize(0, mapper)

    Assertions.assertEquals(
      listOf(
        getStreamStatusCompletedMessage("name1"),
        getStreamStatusCompletedMessage("name2", "namespace2"),
      ),
      result,
    )
  }

  @Test
  fun `test that we support multiple completed status`() {
    streamStatusCompletionTracker.startTracking(catalog, true)
    streamStatusCompletionTracker.track(getStreamStatusCompletedMessage("name1").trace.streamStatus)
    streamStatusCompletionTracker.track(getStreamStatusCompletedMessage("name1").trace.streamStatus)
    val result = streamStatusCompletionTracker.finalize(0, mapper)

    Assertions.assertEquals(
      listOf(
        getStreamStatusCompletedMessage("name1"),
        getStreamStatusCompletedMessage("name2", "namespace2"),
      ),
      result,
    )
  }

  @Test
  fun `test that we get no streams if the exit code is 1 and no stream status is send`() {
    streamStatusCompletionTracker.startTracking(catalog, true)
    val result = streamStatusCompletionTracker.finalize(1, mapper)

    Assertions.assertEquals(listOf<AirbyteMessage>(), result)
  }

  @Test
  fun `test that we get not stream status if the exit code is 1 even the source emitted some stream status`() {
    streamStatusCompletionTracker.startTracking(catalog, true)
    streamStatusCompletionTracker.track(getStreamStatusCompletedMessage("name1").trace.streamStatus)
    val result = streamStatusCompletionTracker.finalize(1, mapper)

    Assertions.assertEquals(listOf<AirbyteMessage>(), result)
  }

  @Test
  fun `test that no message is send if the destination doesn't support refreshes`() {
    streamStatusCompletionTracker.startTracking(catalog, false)
    streamStatusCompletionTracker.track(getStreamStatusCompletedMessage("name1").trace.streamStatus)
    val result = streamStatusCompletionTracker.finalize(0, mapper)

    Assertions.assertEquals(listOf<AirbyteMessage>(), result)
  }

  @Test
  fun `test null and empty string namespaces during tracking`() {
    val catalog =
      ConfiguredAirbyteCatalog()
        .withStreams(
          listOf(
            ConfiguredAirbyteStream(
              AirbyteStream(
                name = "name1",
                namespace = "",
                jsonSchema = Jsons.emptyObject(),
                supportedSyncModes = listOf(SyncMode.INCREMENTAL),
              ),
              SyncMode.INCREMENTAL,
              DestinationSyncMode.APPEND,
            ),
            ConfiguredAirbyteStream(
              AirbyteStream(
                name = "name2",
                namespace = "namespace2",
                jsonSchema = Jsons.emptyObject(),
                supportedSyncModes = listOf(SyncMode.INCREMENTAL),
              ),
              SyncMode.INCREMENTAL,
              DestinationSyncMode.APPEND,
            ),
          ),
        )
    val connectorNullNamespace = StreamDescriptor().withName("name1").withNamespace(null)
    val traceMessage =
      AirbyteStreamStatusTraceMessage()
        .withStreamDescriptor(connectorNullNamespace)
        .withStatus(AirbyteStreamStatusTraceMessage.AirbyteStreamStatus.COMPLETE)
    streamStatusCompletionTracker.startTracking(catalog, true)

    assertDoesNotThrow {
      streamStatusCompletionTracker.track(traceMessage)
    }
  }

  private fun getStreamStatusCompletedMessage(
    name: String,
    namespace: String? = null,
  ): AirbyteMessage =
    AirbyteMessage()
      .withType(AirbyteMessage.Type.TRACE)
      .withTrace(
        AirbyteTraceMessage()
          .withType(AirbyteTraceMessage.Type.STREAM_STATUS)
          .withEmittedAt(1.0)
          .withStreamStatus(
            AirbyteStreamStatusTraceMessage()
              .withStatus(AirbyteStreamStatusTraceMessage.AirbyteStreamStatus.COMPLETE)
              .withStreamDescriptor(
                StreamDescriptor()
                  .withName(name)
                  .withNamespace(namespace),
              ),
          ),
      )
}
