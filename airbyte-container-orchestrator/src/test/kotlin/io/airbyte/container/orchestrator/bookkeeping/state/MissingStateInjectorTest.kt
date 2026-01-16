/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.bookkeeping.state

import io.airbyte.commons.json.Jsons
import io.airbyte.config.AirbyteStream
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.DestinationSyncMode
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.SyncMode
import io.airbyte.container.orchestrator.worker.ReplicationContextProvider
import io.airbyte.container.orchestrator.worker.context.ReplicationContext
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStreamState
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.UUID

internal class MissingStateInjectorTest {
  private fun createContext(streams: List<ConfiguredAirbyteStream>): ReplicationContextProvider.Context {
    val catalog = ConfiguredAirbyteCatalog().withStreams(streams)
    val replicationContext =
      ReplicationContext(
        isReset = false,
        connectionId = UUID.randomUUID(),
        sourceId = UUID.randomUUID(),
        destinationId = UUID.randomUUID(),
        jobId = 1L,
        attempt = 1,
        workspaceId = UUID.randomUUID(),
        sourceDefinitionId = UUID.randomUUID(),
        destinationDefinitionId = UUID.randomUUID(),
      )

    return ReplicationContextProvider.Context(
      replicationContext = replicationContext,
      configuredCatalog = catalog,
      supportRefreshes = false,
      replicationInput = mockk<ReplicationInput>(),
    )
  }

  private fun createFullRefreshStream(
    name: String,
    namespace: String? = null,
    isResumable: Boolean = false,
  ): ConfiguredAirbyteStream =
    ConfiguredAirbyteStream(
      stream =
        AirbyteStream(
          name = name,
          namespace = namespace,
          isResumable = isResumable,
          jsonSchema = Jsons.emptyObject(),
          supportedSyncModes = listOf(SyncMode.FULL_REFRESH),
          sourceDefinedCursor = null,
          defaultCursorField = null,
          sourceDefinedPrimaryKey = null,
          isFileBased = false,
        ),
      syncMode = SyncMode.FULL_REFRESH,
      destinationSyncMode = DestinationSyncMode.OVERWRITE,
    )

  private fun createIncrementalStream(
    name: String,
    namespace: String? = null,
  ): ConfiguredAirbyteStream =
    ConfiguredAirbyteStream(
      stream =
        AirbyteStream(
          name = name,
          namespace = namespace,
          isResumable = true,
          jsonSchema = Jsons.emptyObject(),
          supportedSyncModes = listOf(SyncMode.INCREMENTAL),
          sourceDefinedCursor = null,
          defaultCursorField = null,
          sourceDefinedPrimaryKey = null,
          isFileBased = false,
        ),
      syncMode = SyncMode.INCREMENTAL,
      destinationSyncMode = DestinationSyncMode.APPEND,
    )

  private fun createStateMessage(
    streamName: String,
    namespace: String? = null,
    isGlobal: Boolean = false,
  ): AirbyteMessage =
    if (isGlobal) {
      AirbyteMessage()
        .withType(AirbyteMessage.Type.STATE)
        .withState(
          AirbyteStateMessage()
            .withType(AirbyteStateMessage.AirbyteStateType.GLOBAL)
            .withGlobal(mockk()),
        )
    } else {
      AirbyteMessage()
        .withType(AirbyteMessage.Type.STATE)
        .withState(
          AirbyteStateMessage()
            .withType(AirbyteStateMessage.AirbyteStateType.STREAM)
            .withStream(
              AirbyteStreamState()
                .withStreamDescriptor(
                  io.airbyte.protocol.models.v0
                    .StreamDescriptor()
                    .withName(streamName)
                    .withNamespace(namespace),
                ).withStreamState(Jsons.jsonNode(mapOf("cursor" to "value"))),
            ),
        )
    }

  @Test
  fun `should identify full refresh streams that need state injection`() {
    val stream1 = createFullRefreshStream("users")
    val stream2 = createFullRefreshStream("orders", "public")
    val stream3 = createIncrementalStream("logs") // Should be ignored
    val stream4 = createFullRefreshStream("events", isResumable = true) // Should be ignored

    val context = createContext(listOf(stream1, stream2, stream3, stream4))
    val injector = MissingStateInjector(context)

    val statesToInject = injector.getStatesToInject()

    assertEquals(2, statesToInject.size)

    val stateDescriptors =
      statesToInject.map {
        it.state.stream.streamDescriptor.let { desc ->
          StreamDescriptor().withName(desc.name).withNamespace(desc.namespace)
        }
      }

    assertTrue(stateDescriptors.contains(StreamDescriptor().withName("users")))
    assertTrue(stateDescriptors.contains(StreamDescriptor().withName("orders").withNamespace("public")))
  }

  @Test
  fun `should not inject states when stream state message is received`() {
    val stream = createFullRefreshStream("users")
    val context = createContext(listOf(stream))
    val injector = MissingStateInjector(context)

    injector.trackMessage(createStateMessage("users"))

    val statesToInject = injector.getStatesToInject()
    assertEquals(0, statesToInject.size)
  }

  @Test
  fun `should inject state for streams that did not receive state messages`() {
    val stream1 = createFullRefreshStream("users")
    val stream2 = createFullRefreshStream("orders")
    val context = createContext(listOf(stream1, stream2))
    val injector = MissingStateInjector(context)

    injector.trackMessage(createStateMessage("users"))
    // orders stream did not receive a state message

    val statesToInject = injector.getStatesToInject()
    assertEquals(1, statesToInject.size)

    val injectedState = statesToInject[0]
    assertEquals(AirbyteMessage.Type.STATE, injectedState.type)
    assertEquals(AirbyteStateMessage.AirbyteStateType.STREAM, injectedState.state.type)
    assertEquals("orders", injectedState.state.stream.streamDescriptor.name)
    assertEquals(Jsons.emptyObject(), injectedState.state.stream.streamState)
  }

  @Test
  fun `should not inject states when global state message is received`() {
    val stream1 = createFullRefreshStream("users")
    val stream2 = createFullRefreshStream("orders")
    val context = createContext(listOf(stream1, stream2))
    val injector = MissingStateInjector(context)

    injector.trackMessage(createStateMessage("", isGlobal = true))

    val statesToInject = injector.getStatesToInject()
    assertEquals(0, statesToInject.size)
  }

  @Test
  fun `should handle streams with namespaces correctly`() {
    val stream1 = createFullRefreshStream("users", "public")
    val stream2 = createFullRefreshStream("users", "staging")
    val stream3 = createFullRefreshStream("users") // no namespace
    val context = createContext(listOf(stream1, stream2, stream3))
    val injector = MissingStateInjector(context)

    injector.trackMessage(createStateMessage("users", "public"))

    val statesToInject = injector.getStatesToInject()
    assertEquals(2, statesToInject.size)

    val stateDescriptors =
      statesToInject
        .map {
          it.state.stream.streamDescriptor.let { desc ->
            desc.name to desc.namespace
          }
        }.toSet()

    assertTrue(stateDescriptors.contains("users" to "staging"))
    assertTrue(stateDescriptors.contains("users" to null))
  }

  @Test
  fun `should ignore non-state messages when tracking`() {
    val stream = createFullRefreshStream("users")
    val context = createContext(listOf(stream))
    val injector = MissingStateInjector(context)

    val recordMessage =
      AirbyteMessage()
        .withType(AirbyteMessage.Type.RECORD)
    val logMessage =
      AirbyteMessage()
        .withType(AirbyteMessage.Type.LOG)

    injector.trackMessage(recordMessage)
    injector.trackMessage(logMessage)

    val statesToInject = injector.getStatesToInject()
    assertEquals(1, statesToInject.size) // Should still inject for users stream
  }

  @Test
  fun `should create empty state messages with correct structure`() {
    val stream = createFullRefreshStream("users", "public")
    val context = createContext(listOf(stream))
    val injector = MissingStateInjector(context)

    val statesToInject = injector.getStatesToInject()
    assertEquals(1, statesToInject.size)

    val stateMessage = statesToInject[0]
    assertEquals(AirbyteMessage.Type.STATE, stateMessage.type)
    assertEquals(AirbyteStateMessage.AirbyteStateType.STREAM, stateMessage.state.type)

    val streamState = stateMessage.state.stream
    assertEquals("users", streamState.streamDescriptor.name)
    assertEquals("public", streamState.streamDescriptor.namespace)
    assertEquals(Jsons.emptyObject(), streamState.streamState)
  }

  @Test
  fun `should handle empty catalog`() {
    val context = createContext(emptyList())
    val injector = MissingStateInjector(context)

    val statesToInject = injector.getStatesToInject()
    assertEquals(0, statesToInject.size)
  }

  @Test
  fun `should only consider non-resumable full refresh streams`() {
    val resumableFullRefresh = createFullRefreshStream("resumable", isResumable = true)
    val nonResumableFullRefresh = createFullRefreshStream("non_resumable", isResumable = false)
    val incremental = createIncrementalStream("incremental")

    val context = createContext(listOf(resumableFullRefresh, nonResumableFullRefresh, incremental))
    val injector = MissingStateInjector(context)

    val statesToInject = injector.getStatesToInject()
    assertEquals(1, statesToInject.size)
    assertEquals(
      "non_resumable",
      statesToInject[0]
        .state.stream.streamDescriptor.name,
    )
  }
}
