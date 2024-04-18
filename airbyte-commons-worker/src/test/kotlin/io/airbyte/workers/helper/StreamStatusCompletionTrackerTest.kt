package io.airbyte.workers.helper

import io.airbyte.featureflag.ActivateRefreshes
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.DestinationDefinition
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.SourceDefinition
import io.airbyte.featureflag.Workspace
import io.airbyte.protocol.models.AirbyteMessage
import io.airbyte.protocol.models.AirbyteStream
import io.airbyte.protocol.models.AirbyteStreamStatusTraceMessage
import io.airbyte.protocol.models.AirbyteTraceMessage
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog
import io.airbyte.protocol.models.ConfiguredAirbyteStream
import io.airbyte.protocol.models.StreamDescriptor
import io.airbyte.workers.context.ReplicationContext
import io.airbyte.workers.internal.AirbyteMapper
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Clock
import java.util.UUID

internal class StreamStatusCompletionTrackerTest {
  private val featureFlagClient: FeatureFlagClient = mockk()
  private val clock: Clock = mockk()
  private val mapper: AirbyteMapper = mockk()

  private val streamStatusCompletionTracker = StreamStatusCompletionTracker(featureFlagClient, clock)

  private val catalog =
    ConfiguredAirbyteCatalog()
      .withStreams(
        listOf(
          ConfiguredAirbyteStream().withStream(AirbyteStream().withName("name1")),
          ConfiguredAirbyteStream().withStream(AirbyteStream().withName("name2").withNamespace("namespace2")),
        ),
      )

  private val connectionId = UUID.randomUUID()
  private val workspaceId = UUID.randomUUID()
  private val sourceDefinitionId = UUID.randomUUID()
  private val destinationDefinitionId = UUID.randomUUID()
  private val featureFlagContext =
    Multi(
      listOf(
        Workspace(workspaceId),
        Connection(connectionId),
        SourceDefinition(sourceDefinitionId),
        DestinationDefinition(destinationDefinitionId),
      ),
    )
  private val replicationContext =
    ReplicationContext(
      false,
      connectionId,
      UUID.randomUUID(),
      UUID.randomUUID(),
      0,
      0,
      workspaceId,
      "",
      "",
      sourceDefinitionId,
      destinationDefinitionId,
    )

  @BeforeEach
  fun init() {
    every { clock.millis() } returns 1
    every { mapper.mapMessage(any()) } returnsArgument 0
  }

  @Test
  fun `test that we get all the streams if the exit code is 0 and no stream status is send`() {
    every { featureFlagClient.boolVariation(ActivateRefreshes, featureFlagContext) } returns true

    streamStatusCompletionTracker.startTracking(catalog, replicationContext)
    val result = streamStatusCompletionTracker.finalize(0, mapper)

    assertEquals(
      listOf(
        getStreamStatusCompletedMessage("name1"),
        getStreamStatusCompletedMessage("name2", "namespace2"),
      ),
      result,
    )
  }

  @Test
  fun `test that we get all the streams if the exit code is 0 and some stream status is send`() {
    every { featureFlagClient.boolVariation(ActivateRefreshes, featureFlagContext) } returns true

    streamStatusCompletionTracker.startTracking(catalog, replicationContext)
    streamStatusCompletionTracker.track(getStreamStatusCompletedMessage("name1").trace.streamStatus)
    val result = streamStatusCompletionTracker.finalize(0, mapper)

    assertEquals(
      listOf(
        getStreamStatusCompletedMessage("name1"),
        getStreamStatusCompletedMessage("name2", "namespace2"),
      ),
      result,
    )
  }

  @Test
  fun `test that we get no streams if the exit code is 1 and no stream status is send`() {
    every { featureFlagClient.boolVariation(ActivateRefreshes, featureFlagContext) } returns true

    streamStatusCompletionTracker.startTracking(catalog, replicationContext)
    val result = streamStatusCompletionTracker.finalize(1, mapper)

    assertEquals(listOf<AirbyteMessage>(), result)
  }

  @Test
  fun `test that we get the status of the streams that send a status if the exit code is 1 and no stream status is send`() {
    every { featureFlagClient.boolVariation(ActivateRefreshes, featureFlagContext) } returns true

    streamStatusCompletionTracker.startTracking(catalog, replicationContext)
    streamStatusCompletionTracker.track(getStreamStatusCompletedMessage("name1").trace.streamStatus)
    val result = streamStatusCompletionTracker.finalize(1, mapper)

    assertEquals(
      listOf(
        getStreamStatusCompletedMessage("name1"),
      ),
      result,
    )
  }

  @Test
  fun `test that no message is send if the flag is false`() {
    every { featureFlagClient.boolVariation(ActivateRefreshes, featureFlagContext) } returns false

    streamStatusCompletionTracker.startTracking(catalog, replicationContext)
    streamStatusCompletionTracker.track(getStreamStatusCompletedMessage("name1").trace.streamStatus)
    val result = streamStatusCompletionTracker.finalize(0, mapper)

    assertEquals(listOf<AirbyteMessage>(), result)
  }

  private fun getStreamStatusCompletedMessage(
    name: String,
    namespace: String? = null,
  ): AirbyteMessage {
    return AirbyteMessage()
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
}
