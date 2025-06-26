/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.io

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.collect.Lists
import io.airbyte.commons.json.Jsons
import io.airbyte.config.AirbyteStream
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.DestinationSyncMode
import io.airbyte.config.ResetSourceConfiguration
import io.airbyte.config.State
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.SyncMode
import io.airbyte.config.WorkerSourceConfig
import io.airbyte.config.helpers.ProtocolConverters.Companion.toProtocol
import io.airbyte.protocol.models.v0.AirbyteGlobalState
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStreamState
import io.airbyte.protocol.models.v0.AirbyteStreamStatusTraceMessage
import io.airbyte.protocol.models.v0.AirbyteTraceMessage
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.Optional
import java.util.UUID
import java.util.function.Consumer

private fun getAirbyteStream(name: String): AirbyteStream =
  AirbyteStream(name, Jsons.emptyObject(), listOf(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))

private val AIRBYTE_CATALOG: ConfiguredAirbyteCatalog =
  ConfiguredAirbyteCatalog()
    .withStreams(
      Lists.newArrayList<ConfiguredAirbyteStream>(
        ConfiguredAirbyteStream(getAirbyteStream("a"), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND),
        ConfiguredAirbyteStream(getAirbyteStream("b"), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND),
        ConfiguredAirbyteStream(getAirbyteStream("c"), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND),
      ),
    )

internal class EmptyAirbyteSourceTest {
  private lateinit var emptyAirbyteSource: EmptyAirbyteSource

  @BeforeEach
  fun init() {
    emptyAirbyteSource = EmptyAirbyteSource(hasCustomNamespace = false)
  }

  @Test
  @Throws(Exception::class)
  fun testLegacy() {
    emptyAirbyteSource.start(sourceConfig = WorkerSourceConfig(), jobRoot = null, connectionId = null)

    emptyResult()
  }

  @Test
  @Throws(Exception::class)
  fun testLegacyWithEmptyConfig() {
    emptyAirbyteSource.start(
      sourceConfig =
        WorkerSourceConfig()
          .withSourceConnectionConfiguration(Jsons.emptyObject()),
      jobRoot = null,
      connectionId = null,
    )

    emptyResult()
  }

  @Test
  @Throws(Exception::class)
  fun testLegacyWithWrongConfigFormat() {
    emptyAirbyteSource.start(
      sourceConfig =
        WorkerSourceConfig().withSourceConnectionConfiguration(
          Jsons.jsonNode(mutableMapOf("not" to "expected")),
        ),
      jobRoot = null,
      connectionId = null,
    )

    emptyResult()
  }

  @Test
  @Throws(Exception::class)
  fun testEmptyListOfStreams() {
    val resetSourceConfiguration =
      ResetSourceConfiguration()
        .withStreamsToReset(ArrayList<StreamDescriptor?>())
    val workerSourceConfig =
      WorkerSourceConfig()
        .withSourceConnectionConfiguration(Jsons.jsonNode(resetSourceConfiguration))
        .withCatalog(AIRBYTE_CATALOG)

    emptyAirbyteSource.start(sourceConfig = workerSourceConfig, jobRoot = null, connectionId = null)

    emptyResult()
  }

  @Test
  fun nonStartedSource() {
    assertThrows<IllegalStateException> {
      emptyAirbyteSource.attemptRead()
    }
  }

  @Test
  @Throws(Exception::class)
  fun testGlobal() {
    val streamDescriptors = getProtocolStreamDescriptorFromName(mutableListOf("a", "b", "c"))
    val expectedStreamDescriptors = streamDescriptors.map { obj: StreamDescriptor -> obj.toProtocol() }.toList()

    val streamsToReset = getConfigStreamDescriptorFromName(mutableListOf("a", "b", "c"))

    val resetSourceConfiguration =
      ResetSourceConfiguration()
        .withStreamsToReset(streamsToReset)
    val workerSourceConfig =
      WorkerSourceConfig()
        .withSourceConnectionConfiguration(Jsons.jsonNode(resetSourceConfiguration))
        .withState(
          State()
            .withState(Jsons.jsonNode(createGlobalState(streamDescriptors, Jsons.emptyObject()))),
        ).withCatalog(AIRBYTE_CATALOG)

    emptyAirbyteSource.start(sourceConfig = workerSourceConfig, jobRoot = null, connectionId = null)

    val maybeMessage: Optional<AirbyteMessage> = emptyAirbyteSource.attemptRead()
    assertEquals(true, maybeMessage.isPresent)

    val message = maybeMessage.get()
    assertEquals(AirbyteMessage.Type.STATE, message.type)

    /*
     * The comparison could be what it is below but it makes it hard to see what is the diff. It has
     * been break dow into multiples assertions. (same comment in the other tests)
     *
     * AirbyteStateMessage expectedState = new AirbyteStateMessage()
     * .withStateType(AirbyteStateType.GLOBAL) .withGlobal( new AirbyteGlobalState()
     * .withSharedState(Jsons.emptyObject()) .withStreamStates( Lists.newArrayList( new
     * AirbyteStreamState().withStreamState(null).withStreamDescriptor(new
     * StreamDescriptor().withName("a")), new
     * AirbyteStreamState().withStreamState(null).withStreamDescriptor(new
     * StreamDescriptor().withName("b")), new
     * AirbyteStreamState().withStreamState(null).withStreamDescriptor(new
     * StreamDescriptor().withName("c")) ) ) );
     *
     * Assertions.assertThat(stateMessage).isEqualTo(expectedState);
     */
    val stateMessage = message.state
    assertEquals(AirbyteStateMessage.AirbyteStateType.GLOBAL, stateMessage.type)
    assertNull(stateMessage.global.sharedState)
    assertEquals(expectedStreamDescriptors, stateMessage.global.streamStates.map { state -> state.streamDescriptor })
    assertEquals(
      emptyList<JsonNode>(),
      stateMessage.global.streamStates
        .map { state -> state.streamState }
        .filter { state -> state != null },
    )

    streamsToReset.forEach(Consumer { s: StreamDescriptor? -> this.testReceiveResetMessageTupleForSingleStateTypes(s!!) })

    assertEquals(true, emptyAirbyteSource.attemptRead().isEmpty)
  }

  @Test
  @Throws(Exception::class)
  fun testGlobalPartial() {
    val notResetStreamName = "c"

    val streamDescriptors = getProtocolStreamDescriptorFromName(mutableListOf("a", "b", notResetStreamName))
    val streamsToReset = getConfigStreamDescriptorFromName(mutableListOf("a", "b"))

    val resetSourceConfiguration =
      ResetSourceConfiguration()
        .withStreamsToReset(streamsToReset)
    val workerSourceConfig =
      WorkerSourceConfig()
        .withSourceConnectionConfiguration(Jsons.jsonNode(resetSourceConfiguration))
        .withState(
          State()
            .withState(Jsons.jsonNode(createGlobalState(streamDescriptors, Jsons.emptyObject()))),
        ).withCatalog(AIRBYTE_CATALOG)

    emptyAirbyteSource.start(sourceConfig = workerSourceConfig, jobRoot = null, connectionId = null)

    val maybeMessage: Optional<AirbyteMessage> = emptyAirbyteSource.attemptRead()
    assertEquals(true, maybeMessage.isPresent)

    val message = maybeMessage.get()
    assertEquals(AirbyteMessage.Type.STATE, message.type)

    val stateMessage = message.state

    assertEquals(AirbyteStateMessage.AirbyteStateType.GLOBAL, stateMessage.type)
    assertEquals(Jsons.emptyObject(), stateMessage.global.sharedState)
    assertEquals(
      listOf(Jsons.emptyObject()),
      stateMessage.global.streamStates
        .filter { stream ->
          stream.streamDescriptor.name == notResetStreamName
        }.map { state -> state.streamState }
        .filter { state ->
          state !=
            null
        },
    )

    streamsToReset.forEach(Consumer { s: StreamDescriptor -> this.testReceiveResetMessageTupleForSingleStateTypes(s) })

    assertEquals(true, emptyAirbyteSource.attemptRead().isEmpty)
    assertEquals(true, emptyAirbyteSource.isFinished)
  }

  @Test
  @Throws(Exception::class)
  fun testGlobalNewStream() {
    val newStream = "c"

    val streamDescriptors = getProtocolStreamDescriptorFromName(mutableListOf("a", "b"))

    val streamsToReset = getConfigStreamDescriptorFromName(mutableListOf("a", "b", newStream))

    val resetSourceConfiguration =
      ResetSourceConfiguration()
        .withStreamsToReset(streamsToReset)
    val workerSourceConfig =
      WorkerSourceConfig()
        .withSourceConnectionConfiguration(Jsons.jsonNode(resetSourceConfiguration))
        .withState(
          State()
            .withState(Jsons.jsonNode(createGlobalState(streamDescriptors, Jsons.emptyObject()))),
        ).withCatalog(AIRBYTE_CATALOG)

    emptyAirbyteSource.start(sourceConfig = workerSourceConfig, jobRoot = null, connectionId = null)

    val maybeMessage: Optional<AirbyteMessage> = emptyAirbyteSource.attemptRead()
    assertEquals(true, maybeMessage.isPresent)

    val message = maybeMessage.get()
    assertEquals(AirbyteMessage.Type.STATE, message.type)

    val stateMessage = message.state

    assertEquals(AirbyteStateMessage.AirbyteStateType.GLOBAL, stateMessage.type)
    assertEquals(null, stateMessage.global.sharedState)
    assertEquals(
      emptyList<JsonNode>(),
      stateMessage.global.streamStates
        .map { state -> state.streamState }
        .filter { state -> state != null },
    )
    assertEquals(
      1,
      stateMessage.global.streamStates
        .filter { stream -> stream.streamDescriptor.name == newStream }
        .size,
    )

    streamsToReset.forEach(Consumer { s: StreamDescriptor -> this.testReceiveResetMessageTupleForSingleStateTypes(s) })

    assertEquals(true, emptyAirbyteSource.attemptRead().isEmpty)
    assertEquals(true, emptyAirbyteSource.isFinished)
  }

  @Test
  @Throws(Exception::class)
  fun testPerStream() {
    val streamDescriptors = getProtocolStreamDescriptorFromName(mutableListOf("a", "b", "c"))

    val streamsToReset = getConfigStreamDescriptorFromName(mutableListOf("a", "b", "c"))

    val resetSourceConfiguration =
      ResetSourceConfiguration()
        .withStreamsToReset(streamsToReset)
    val workerSourceConfig =
      WorkerSourceConfig()
        .withSourceConnectionConfiguration(Jsons.jsonNode(resetSourceConfiguration))
        .withState(
          State()
            .withState(Jsons.jsonNode(createPerStreamState(streamDescriptors))),
        ).withCatalog(AIRBYTE_CATALOG)

    emptyAirbyteSource.start(sourceConfig = workerSourceConfig, jobRoot = null, connectionId = null)

    streamsToReset.forEach(Consumer { s: StreamDescriptor -> testReceiveExpectedPerStreamMessages(s, true) })

    assertEquals(true, emptyAirbyteSource.attemptRead().isEmpty)
    assertEquals(true, emptyAirbyteSource.isFinished)
  }

  @Test
  @Throws(Exception::class)
  fun testPerStreamCustomFormat() {
    emptyAirbyteSource = EmptyAirbyteSource(true)

    val streamDescriptors = getProtocolStreamDescriptorFromName(mutableListOf("a", "b", "c"))

    val streamsToReset = getConfigStreamDescriptorFromName(mutableListOf("a", "b", "c"))

    val resetSourceConfiguration =
      ResetSourceConfiguration()
        .withStreamsToReset(streamsToReset)
    val workerSourceConfig =
      WorkerSourceConfig()
        .withSourceConnectionConfiguration(Jsons.jsonNode(resetSourceConfiguration))
        .withState(
          State()
            .withState(Jsons.jsonNode(createPerStreamState(streamDescriptors))),
        ).withCatalog(AIRBYTE_CATALOG)

    emptyAirbyteSource.start(sourceConfig = workerSourceConfig, jobRoot = null, connectionId = null)

    streamsToReset.forEach(Consumer { s: StreamDescriptor -> testReceiveExpectedPerStreamMessages(s, false) })

    assertEquals(true, emptyAirbyteSource.attemptRead().isEmpty)
    assertEquals(true, emptyAirbyteSource.isFinished)
  }

  @Test
  @Throws(Exception::class)
  fun testPerStreamWithExtraState() {
    // This should never happen but nothing keeps us from processing the reset and not fail
    val streamDescriptors = getProtocolStreamDescriptorFromName(mutableListOf("a", "b", "c", "d"))

    val streamsToReset = getConfigStreamDescriptorFromName(mutableListOf("a", "b", "c"))

    val resetSourceConfiguration =
      ResetSourceConfiguration()
        .withStreamsToReset(streamsToReset)
    val workerSourceConfig =
      WorkerSourceConfig()
        .withSourceConnectionConfiguration(Jsons.jsonNode(resetSourceConfiguration))
        .withState(
          State()
            .withState(Jsons.jsonNode(createPerStreamState(streamDescriptors))),
        ).withCatalog(AIRBYTE_CATALOG)

    emptyAirbyteSource.start(sourceConfig = workerSourceConfig, jobRoot = null, connectionId = null)

    streamsToReset.forEach(Consumer { s: StreamDescriptor -> testReceiveExpectedPerStreamMessages(s, true) })

    assertEquals(true, emptyAirbyteSource.attemptRead().isEmpty)
    assertEquals(true, emptyAirbyteSource.isFinished)
  }

  @Test
  @Throws(Exception::class)
  fun testPerStreamWithMissingState() {
    val newStream = "c"

    val streamDescriptors = getProtocolStreamDescriptorFromName(mutableListOf("a", "b"))

    val streamsToReset = getConfigStreamDescriptorFromName(mutableListOf("a", "b", newStream))

    val resetSourceConfiguration =
      ResetSourceConfiguration()
        .withStreamsToReset(streamsToReset)
    val workerSourceConfig =
      WorkerSourceConfig()
        .withSourceConnectionConfiguration(Jsons.jsonNode(resetSourceConfiguration))
        .withState(
          State()
            .withState(Jsons.jsonNode(createPerStreamState(streamDescriptors))),
        ).withCatalog(AIRBYTE_CATALOG)

    emptyAirbyteSource.start(sourceConfig = workerSourceConfig, jobRoot = null, connectionId = null)

    streamsToReset.forEach(Consumer { s: StreamDescriptor -> testReceiveExpectedPerStreamMessages(s, true) })

    assertEquals(true, emptyAirbyteSource.attemptRead().isEmpty)
    assertEquals(true, emptyAirbyteSource.isFinished)
  }

  // In the LEGACY state, if the list of streams passed in to be reset does not include every stream
  // in the Catalog, then something has gone wrong and we should throw an error
  @Test
  fun testLegacyWithMissingCatalogStream() {
    val streamsToReset = getConfigStreamDescriptorFromName(mutableListOf("a", "b", "c"))

    val resetSourceConfiguration =
      ResetSourceConfiguration()
        .withStreamsToReset(streamsToReset)
    val airbyteCatalogWithExtraStream =
      ConfiguredAirbyteCatalog()
        .withStreams(
          mutableListOf(
            ConfiguredAirbyteStream(getAirbyteStream("a"), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND),
            ConfiguredAirbyteStream(getAirbyteStream("b"), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND),
            ConfiguredAirbyteStream(getAirbyteStream("c"), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND),
            ConfiguredAirbyteStream(getAirbyteStream("d"), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND),
          ),
        )

    val workerSourceConfig =
      WorkerSourceConfig()
        .withSourceConnectionConfiguration(Jsons.jsonNode(resetSourceConfiguration))
        .withState(
          State()
            .withState(Jsons.emptyObject()),
        ).withCatalog(airbyteCatalogWithExtraStream)

    assertThrows<IllegalStateException> { emptyAirbyteSource.start(sourceConfig = workerSourceConfig, jobRoot = null, connectionId = null) }
  }

  // If there are extra streams to reset that do not exist in the Catalog, the reset should work
  // properly with all streams being reset
  @Test
  @Throws(Exception::class)
  fun testLegacyWithResettingExtraStreamNotInCatalog() {
    val streamsToResetWithExtra = getConfigStreamDescriptorFromName(mutableListOf("a", "b", "c", "d"))

    val resetSourceConfiguration =
      ResetSourceConfiguration()
        .withStreamsToReset(streamsToResetWithExtra)
    val airbyteCatalog =
      ConfiguredAirbyteCatalog()
        .withStreams(
          mutableListOf(
            ConfiguredAirbyteStream(getAirbyteStream("a"), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND),
            ConfiguredAirbyteStream(getAirbyteStream("b"), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND),
            ConfiguredAirbyteStream(getAirbyteStream("c"), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND),
          ),
        )

    val workerSourceConfig =
      WorkerSourceConfig()
        .withSourceConnectionConfiguration(Jsons.jsonNode(resetSourceConfiguration))
        .withState(
          State()
            .withState(Jsons.jsonNode(mapOf("cursor" to "1"))),
        ).withCatalog(airbyteCatalog)

    emptyAirbyteSource.start(sourceConfig = workerSourceConfig, jobRoot = null, connectionId = null)

    val maybeMessage: Optional<AirbyteMessage> = emptyAirbyteSource.attemptRead()
    assertEquals(true, maybeMessage.isPresent)

    val message = maybeMessage.get()
    assertEquals(AirbyteMessage.Type.STATE, message.type)

    val stateMessage = message.state
    assertEquals(AirbyteStateMessage.AirbyteStateType.LEGACY, stateMessage.type)
    assertEquals(Jsons.emptyObject(), stateMessage.data)

    streamsToResetWithExtra.forEach(Consumer { s: StreamDescriptor -> this.testReceiveResetMessageTupleForSingleStateTypes(s) })

    assertEquals(true, emptyAirbyteSource.attemptRead().isEmpty)
    assertEquals(true, emptyAirbyteSource.isFinished)
  }

  @Test
  @Throws(Exception::class)
  fun testLegacyWithNewConfig() {
    val streamsToReset = getConfigStreamDescriptorFromName(mutableListOf("a", "b", "c"))

    val resetSourceConfiguration =
      ResetSourceConfiguration()
        .withStreamsToReset(streamsToReset)
    val airbyteCatalog =
      ConfiguredAirbyteCatalog()
        .withStreams(
          mutableListOf(
            ConfiguredAirbyteStream(getAirbyteStream("a"), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND),
            ConfiguredAirbyteStream(getAirbyteStream("b"), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND),
            ConfiguredAirbyteStream(getAirbyteStream("c"), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND),
          ),
        )

    val workerSourceConfig =
      WorkerSourceConfig()
        .withSourceConnectionConfiguration(Jsons.jsonNode(resetSourceConfiguration))
        .withState(
          State()
            .withState(Jsons.jsonNode(mutableMapOf("cursor" to "1"))),
        ).withCatalog(airbyteCatalog)

    emptyAirbyteSource.start(sourceConfig = workerSourceConfig, jobRoot = null, connectionId = null)

    val maybeMessage: Optional<AirbyteMessage> = emptyAirbyteSource.attemptRead()
    assertEquals(true, maybeMessage.isPresent)

    val message = maybeMessage.get()
    assertEquals(AirbyteMessage.Type.STATE, message.type)

    val stateMessage = message.state
    assertEquals(AirbyteStateMessage.AirbyteStateType.LEGACY, stateMessage.type)
    assertEquals(Jsons.emptyObject(), stateMessage.data)

    streamsToReset.forEach(Consumer { s: StreamDescriptor -> this.testReceiveResetMessageTupleForSingleStateTypes(s) })

    assertEquals(true, emptyAirbyteSource.attemptRead().isEmpty)
    assertEquals(true, emptyAirbyteSource.isFinished)
  }

  @Test
  @Throws(Exception::class)
  fun testLegacyWithNewConfigWithCustomFormat() {
    emptyAirbyteSource = EmptyAirbyteSource(true)
    val streamsToReset = getConfigStreamDescriptorFromName(mutableListOf("a", "b", "c"))

    val resetSourceConfiguration =
      ResetSourceConfiguration()
        .withStreamsToReset(streamsToReset)
    val airbyteCatalog =
      ConfiguredAirbyteCatalog()
        .withStreams(
          mutableListOf(
            ConfiguredAirbyteStream(getAirbyteStream("a"), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND),
            ConfiguredAirbyteStream(getAirbyteStream("b"), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND),
            ConfiguredAirbyteStream(getAirbyteStream("c"), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND),
          ),
        )

    val workerSourceConfig =
      WorkerSourceConfig()
        .withSourceConnectionConfiguration(Jsons.jsonNode(resetSourceConfiguration))
        .withState(
          State()
            .withState(Jsons.jsonNode(mutableMapOf("cursor" to "1"))),
        ).withCatalog(airbyteCatalog)

    emptyAirbyteSource.start(sourceConfig = workerSourceConfig, jobRoot = null, connectionId = null)

    val maybeMessage: Optional<AirbyteMessage> = emptyAirbyteSource.attemptRead()
    assertEquals(true, maybeMessage.isPresent)

    val message = maybeMessage.get()
    assertEquals(AirbyteMessage.Type.STATE, message.type)

    val stateMessage = message.state
    assertEquals(AirbyteStateMessage.AirbyteStateType.LEGACY, stateMessage.type)
    assertEquals(Jsons.emptyObject(), stateMessage.data)

    assertEquals(true, emptyAirbyteSource.attemptRead().isEmpty)
    assertEquals(true, emptyAirbyteSource.isFinished)
  }

  @Test
  @Throws(Exception::class)
  fun testLegacyWithNullState() {
    val streamsToReset = getConfigStreamDescriptorFromName(mutableListOf("a", "b", "c"))

    val resetSourceConfiguration =
      ResetSourceConfiguration()
        .withStreamsToReset(streamsToReset)
    val airbyteCatalogWithExtraStream =
      ConfiguredAirbyteCatalog()
        .withStreams(
          mutableListOf(
            ConfiguredAirbyteStream(getAirbyteStream("a"), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND),
            ConfiguredAirbyteStream(getAirbyteStream("b"), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND),
            ConfiguredAirbyteStream(getAirbyteStream("c"), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND),
          ),
        )

    val workerSourceConfig =
      WorkerSourceConfig()
        .withSourceConnectionConfiguration(Jsons.jsonNode(resetSourceConfiguration))
        .withCatalog(airbyteCatalogWithExtraStream)

    emptyAirbyteSource.start(sourceConfig = workerSourceConfig, jobRoot = null, connectionId = null)

    streamsToReset.forEach(Consumer { s: StreamDescriptor -> this.testReceiveResetMessageTupleForSingleStateTypes(s) })

    val maybeMessage: Optional<AirbyteMessage> = emptyAirbyteSource.attemptRead()
    assertEquals(true, maybeMessage.isEmpty)
    assertEquals(true, emptyAirbyteSource.isFinished)
  }

  private fun testReceiveResetStatusMessage(
    streamDescriptor: StreamDescriptor,
    status: AirbyteStreamStatusTraceMessage.AirbyteStreamStatus,
  ) {
    val maybeMessage: Optional<AirbyteMessage> = emptyAirbyteSource.attemptRead()
    assertEquals(true, maybeMessage.isPresent)

    val message = maybeMessage.get()
    assertEquals(AirbyteMessage.Type.TRACE, message.type)
    assertEquals(AirbyteTraceMessage.Type.STREAM_STATUS, message.trace.type)
    assertEquals(status, message.trace.streamStatus.status)

    val expectedStreamDescriptor =
      io.airbyte.protocol.models.v0
        .StreamDescriptor()
        .withName(streamDescriptor.name)
        .withNamespace(streamDescriptor.namespace)
    assertEquals(expectedStreamDescriptor, message.trace.streamStatus.streamDescriptor)
  }

  private fun testReceiveNullStreamStateMessage(streamDescriptor: StreamDescriptor) {
    val maybeMessage: Optional<AirbyteMessage> = emptyAirbyteSource.attemptRead()
    assertEquals(true, maybeMessage.isPresent)

    val message = maybeMessage.get()
    assertEquals(AirbyteMessage.Type.STATE, message.getType())

    val expectedStreamDescriptor =
      io.airbyte.protocol.models.v0
        .StreamDescriptor()
        .withName(streamDescriptor.name)
        .withNamespace(streamDescriptor.namespace)
    val stateMessage = message.state
    assertEquals(AirbyteStateMessage.AirbyteStateType.STREAM, stateMessage.type)
    assertEquals(expectedStreamDescriptor, stateMessage.stream.streamDescriptor)
    assertEquals(null, stateMessage.stream.streamState)
  }

  private fun testReceiveExpectedPerStreamMessages(
    s: StreamDescriptor,
    includeStatus: Boolean,
  ) {
    if (includeStatus) {
      testReceiveResetStatusMessage(s, AirbyteStreamStatusTraceMessage.AirbyteStreamStatus.STARTED)
    }
    testReceiveNullStreamStateMessage(s)
    if (includeStatus) {
      testReceiveResetStatusMessage(s, AirbyteStreamStatusTraceMessage.AirbyteStreamStatus.COMPLETE)
    }
  }

  private fun testReceiveResetMessageTupleForSingleStateTypes(s: StreamDescriptor) {
    testReceiveResetStatusMessage(s, AirbyteStreamStatusTraceMessage.AirbyteStreamStatus.STARTED)
    testReceiveResetStatusMessage(s, AirbyteStreamStatusTraceMessage.AirbyteStreamStatus.COMPLETE)
  }

  private fun getProtocolStreamDescriptorFromName(names: MutableList<String>): MutableList<StreamDescriptor> =
    names.map { name: String -> StreamDescriptor().withName(name) }.toMutableList()

  private fun getConfigStreamDescriptorFromName(names: MutableList<String>): MutableList<StreamDescriptor> =
    names.map { name: String -> StreamDescriptor().withName(name) }.toMutableList()

  private fun emptyResult() {
    assertEquals(true, emptyAirbyteSource.attemptRead().isEmpty)
  }

  private fun createPerStreamState(streamDescriptors: MutableList<StreamDescriptor>): MutableList<AirbyteStateMessage> =
    streamDescriptors
      .map { streamDescriptor: StreamDescriptor ->
        AirbyteStateMessage()
          .withType(AirbyteStateMessage.AirbyteStateType.STREAM)
          .withStream(
            AirbyteStreamState()
              .withStreamDescriptor(streamDescriptor.toProtocol())
              .withStreamState(Jsons.emptyObject()),
          )
      }.toMutableList()

  private fun createGlobalState(
    streamDescriptors: MutableList<StreamDescriptor>,
    sharedState: JsonNode,
  ): MutableList<AirbyteStateMessage> {
    val globalState =
      AirbyteGlobalState()
        .withSharedState(sharedState)
        .withStreamStates(
          streamDescriptors
            .map { streamDescriptor: StreamDescriptor ->
              AirbyteStreamState()
                .withStreamDescriptor(streamDescriptor.toProtocol())
                .withStreamState(Jsons.emptyObject())
            }.toList(),
        )

    return mutableListOf(
      AirbyteStateMessage()
        .withType(AirbyteStateMessage.AirbyteStateType.GLOBAL)
        .withGlobal(globalState),
    )
  }

  @Test
  @Throws(Exception::class)
  fun emptyLegacyStateShouldNotEmitState() {
    val streamDescriptor = StreamDescriptor().withName("test").withNamespace("schema")
    val resetSourceConfiguration =
      ResetSourceConfiguration().withStreamsToReset(mutableListOf<StreamDescriptor?>(streamDescriptor))
    val configuredAirbyteCatalog =
      ConfiguredAirbyteCatalog()
        .withStreams(
          mutableListOf(
            ConfiguredAirbyteStream(
              getAirbyteStream("test").withNamespace("schema"),
              SyncMode.INCREMENTAL,
              DestinationSyncMode.APPEND,
            ),
          ),
        )
    val workerSourceConfig =
      WorkerSourceConfig()
        .withSourceId(UUID.randomUUID())
        .withState(State().withState(Jsons.emptyObject()))
        .withCatalog(configuredAirbyteCatalog)
        .withSourceConnectionConfiguration(Jsons.jsonNode(resetSourceConfiguration))

    emptyAirbyteSource.start(sourceConfig = workerSourceConfig, jobRoot = null, connectionId = null)
    testReceiveResetMessageTupleForSingleStateTypes(streamDescriptor)
    assertEquals(true, emptyAirbyteSource.attemptRead().isEmpty)
    assertEquals(true, emptyAirbyteSource.isFinished)
  }
}
