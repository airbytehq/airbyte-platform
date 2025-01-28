/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.AirbyteStream;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.ConfiguredAirbyteStream;
import io.airbyte.config.DestinationSyncMode;
import io.airbyte.config.ResetSourceConfiguration;
import io.airbyte.config.State;
import io.airbyte.config.StreamDescriptor;
import io.airbyte.config.SyncMode;
import io.airbyte.config.WorkerSourceConfig;
import io.airbyte.config.helpers.ProtocolConverters;
import io.airbyte.protocol.models.AirbyteGlobalState;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.airbyte.protocol.models.AirbyteStateMessage.AirbyteStateType;
import io.airbyte.protocol.models.AirbyteStreamState;
import io.airbyte.protocol.models.AirbyteStreamStatusTraceMessage.AirbyteStreamStatus;
import io.airbyte.protocol.models.AirbyteTraceMessage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
class EmptyAirbyteSourceTest {

  private EmptyAirbyteSource emptyAirbyteSource;

  private static final ConfiguredAirbyteCatalog AIRBYTE_CATALOG = new ConfiguredAirbyteCatalog()
      .withStreams(Lists.newArrayList(
          new ConfiguredAirbyteStream(getAirbyteStream("a"), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND),
          new ConfiguredAirbyteStream(getAirbyteStream("b"), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND),
          new ConfiguredAirbyteStream(getAirbyteStream("c"), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND)));

  @BeforeEach
  void init() {
    emptyAirbyteSource = new EmptyAirbyteSource(false);
  }

  @Test
  void testLegacy() throws Exception {
    emptyAirbyteSource.start(new WorkerSourceConfig(), null, null);

    emptyResult();
  }

  @Test
  void testLegacyWithEmptyConfig() throws Exception {
    emptyAirbyteSource.start(new WorkerSourceConfig().withSourceConnectionConfiguration(Jsons.emptyObject()), null, null);

    emptyResult();
  }

  @Test
  void testLegacyWithWrongConfigFormat() throws Exception {
    emptyAirbyteSource.start(new WorkerSourceConfig().withSourceConnectionConfiguration(Jsons.jsonNode(
        Map.of("not", "expected"))), null, null);

    emptyResult();
  }

  @Test
  void testEmptyListOfStreams() throws Exception {
    final ResetSourceConfiguration resetSourceConfiguration = new ResetSourceConfiguration()
        .withStreamsToReset(new ArrayList<>());
    final WorkerSourceConfig workerSourceConfig = new WorkerSourceConfig()
        .withSourceConnectionConfiguration(Jsons.jsonNode(resetSourceConfiguration))
        .withCatalog(AIRBYTE_CATALOG);

    emptyAirbyteSource.start(workerSourceConfig, null, null);

    emptyResult();
  }

  @Test
  void nonStartedSource() {
    final Throwable thrown = Assertions.catchThrowable(emptyAirbyteSource::attemptRead);
    Assertions.assertThat(thrown)
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void testGlobal() throws Exception {
    final List<StreamDescriptor> streamDescriptors = getProtocolStreamDescriptorFromName(Lists.newArrayList("a", "b", "c"));
    final List<io.airbyte.protocol.models.StreamDescriptor> expectedStreamDescriptors =
        streamDescriptors.stream().map(ProtocolConverters::toProtocol).toList();

    final List<StreamDescriptor> streamsToReset = getConfigStreamDescriptorFromName(Lists.newArrayList("a", "b", "c"));

    final ResetSourceConfiguration resetSourceConfiguration = new ResetSourceConfiguration()
        .withStreamsToReset(streamsToReset);
    final WorkerSourceConfig workerSourceConfig = new WorkerSourceConfig()
        .withSourceConnectionConfiguration(Jsons.jsonNode(resetSourceConfiguration))
        .withState(new State()
            .withState(Jsons.jsonNode(createGlobalState(streamDescriptors, Jsons.emptyObject()))))
        .withCatalog(AIRBYTE_CATALOG);

    emptyAirbyteSource.start(workerSourceConfig, null, null);

    final Optional<AirbyteMessage> maybeMessage = emptyAirbyteSource.attemptRead();
    Assertions.assertThat(maybeMessage)
        .isNotEmpty();

    final AirbyteMessage message = maybeMessage.get();
    Assertions.assertThat(message.getType()).isEqualTo(Type.STATE);

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
    final AirbyteStateMessage stateMessage = message.getState();
    Assertions.assertThat(stateMessage.getType()).isEqualTo(AirbyteStateType.GLOBAL);
    Assertions.assertThat(stateMessage.getGlobal().getSharedState()).isNull();
    Assertions.assertThat(stateMessage.getGlobal().getStreamStates())
        .map(AirbyteStreamState::getStreamDescriptor)
        .containsExactlyElementsOf(expectedStreamDescriptors);
    Assertions.assertThat(stateMessage.getGlobal().getStreamStates())
        .map(AirbyteStreamState::getStreamState)
        .containsOnlyNulls();

    streamsToReset.forEach(this::testReceiveResetMessageTupleForSingleStateTypes);

    Assertions.assertThat(emptyAirbyteSource.attemptRead())
        .isEmpty();
  }

  @Test
  void testGlobalPartial() throws Exception {
    final String notResetStreamName = "c";

    final List<StreamDescriptor> streamDescriptors = getProtocolStreamDescriptorFromName(Lists.newArrayList("a", "b", notResetStreamName));

    final List<StreamDescriptor> streamsToReset = getConfigStreamDescriptorFromName(Lists.newArrayList("a", "b"));

    final ResetSourceConfiguration resetSourceConfiguration = new ResetSourceConfiguration()
        .withStreamsToReset(streamsToReset);
    final WorkerSourceConfig workerSourceConfig = new WorkerSourceConfig()
        .withSourceConnectionConfiguration(Jsons.jsonNode(resetSourceConfiguration))
        .withState(new State()
            .withState(Jsons.jsonNode(createGlobalState(streamDescriptors, Jsons.emptyObject()))))
        .withCatalog(AIRBYTE_CATALOG);

    emptyAirbyteSource.start(workerSourceConfig, null, null);

    final Optional<AirbyteMessage> maybeMessage = emptyAirbyteSource.attemptRead();
    Assertions.assertThat(maybeMessage)
        .isNotEmpty();

    final AirbyteMessage message = maybeMessage.get();
    Assertions.assertThat(message.getType()).isEqualTo(Type.STATE);

    final AirbyteStateMessage stateMessage = message.getState();

    Assertions.assertThat(stateMessage.getType()).isEqualTo(AirbyteStateType.GLOBAL);
    Assertions.assertThat(stateMessage.getGlobal().getSharedState()).isEqualTo(Jsons.emptyObject());
    Assertions.assertThat(stateMessage.getGlobal().getStreamStates())
        .filteredOn(streamState -> !streamState.getStreamDescriptor().getName().equals(notResetStreamName))
        .map(AirbyteStreamState::getStreamState)
        .containsOnlyNulls();
    Assertions.assertThat(stateMessage.getGlobal().getStreamStates())
        .filteredOn(streamState -> streamState.getStreamDescriptor().getName().equals(notResetStreamName))
        .map(AirbyteStreamState::getStreamState)
        .contains(Jsons.emptyObject());

    streamsToReset.forEach(this::testReceiveResetMessageTupleForSingleStateTypes);

    Assertions.assertThat(emptyAirbyteSource.attemptRead())
        .isEmpty();

    Assertions.assertThat(emptyAirbyteSource.isFinished()).isTrue();
  }

  @Test
  void testGlobalNewStream() throws Exception {
    final String newStream = "c";

    final List<StreamDescriptor> streamDescriptors = getProtocolStreamDescriptorFromName(Lists.newArrayList("a", "b"));

    final List<StreamDescriptor> streamsToReset = getConfigStreamDescriptorFromName(Lists.newArrayList("a", "b", newStream));

    final ResetSourceConfiguration resetSourceConfiguration = new ResetSourceConfiguration()
        .withStreamsToReset(streamsToReset);
    final WorkerSourceConfig workerSourceConfig = new WorkerSourceConfig()
        .withSourceConnectionConfiguration(Jsons.jsonNode(resetSourceConfiguration))
        .withState(new State()
            .withState(Jsons.jsonNode(createGlobalState(streamDescriptors, Jsons.emptyObject()))))
        .withCatalog(AIRBYTE_CATALOG);

    emptyAirbyteSource.start(workerSourceConfig, null, null);

    final Optional<AirbyteMessage> maybeMessage = emptyAirbyteSource.attemptRead();
    Assertions.assertThat(maybeMessage)
        .isNotEmpty();

    final AirbyteMessage message = maybeMessage.get();
    Assertions.assertThat(message.getType()).isEqualTo(Type.STATE);

    final AirbyteStateMessage stateMessage = message.getState();

    Assertions.assertThat(stateMessage.getType()).isEqualTo(AirbyteStateType.GLOBAL);
    Assertions.assertThat(stateMessage.getGlobal().getSharedState()).isNull();
    Assertions.assertThat(stateMessage.getGlobal().getStreamStates())
        .map(AirbyteStreamState::getStreamState)
        .containsOnlyNulls();
    Assertions.assertThat(stateMessage.getGlobal().getStreamStates())
        .filteredOn(streamState -> streamState.getStreamDescriptor().getName().equals(newStream))
        .hasSize(1);

    streamsToReset.forEach(this::testReceiveResetMessageTupleForSingleStateTypes);

    Assertions.assertThat(emptyAirbyteSource.attemptRead())
        .isEmpty();

    Assertions.assertThat(emptyAirbyteSource.isFinished()).isTrue();
  }

  @Test
  void testPerStream() throws Exception {
    final List<StreamDescriptor> streamDescriptors = getProtocolStreamDescriptorFromName(Lists.newArrayList("a", "b", "c"));

    final List<StreamDescriptor> streamsToReset = getConfigStreamDescriptorFromName(Lists.newArrayList("a", "b", "c"));

    final ResetSourceConfiguration resetSourceConfiguration = new ResetSourceConfiguration()
        .withStreamsToReset(streamsToReset);
    final WorkerSourceConfig workerSourceConfig = new WorkerSourceConfig()
        .withSourceConnectionConfiguration(Jsons.jsonNode(resetSourceConfiguration))
        .withState(new State()
            .withState(Jsons.jsonNode(createPerStreamState(streamDescriptors))))
        .withCatalog(AIRBYTE_CATALOG);

    emptyAirbyteSource.start(workerSourceConfig, null, null);

    streamsToReset.forEach(s -> testReceiveExpectedPerStreamMessages(s, true));

    Assertions.assertThat(emptyAirbyteSource.attemptRead())
        .isEmpty();

    Assertions.assertThat(emptyAirbyteSource.isFinished()).isTrue();
  }

  @Test
  void testPerStreamCustomFormat() throws Exception {
    emptyAirbyteSource = new EmptyAirbyteSource(true);

    final List<StreamDescriptor> streamDescriptors = getProtocolStreamDescriptorFromName(Lists.newArrayList("a", "b", "c"));

    final List<StreamDescriptor> streamsToReset = getConfigStreamDescriptorFromName(Lists.newArrayList("a", "b", "c"));

    final ResetSourceConfiguration resetSourceConfiguration = new ResetSourceConfiguration()
        .withStreamsToReset(streamsToReset);
    final WorkerSourceConfig workerSourceConfig = new WorkerSourceConfig()
        .withSourceConnectionConfiguration(Jsons.jsonNode(resetSourceConfiguration))
        .withState(new State()
            .withState(Jsons.jsonNode(createPerStreamState(streamDescriptors))))
        .withCatalog(AIRBYTE_CATALOG);

    emptyAirbyteSource.start(workerSourceConfig, null, null);

    streamsToReset.forEach(s -> testReceiveExpectedPerStreamMessages(s, false));

    Assertions.assertThat(emptyAirbyteSource.attemptRead())
        .isEmpty();

    Assertions.assertThat(emptyAirbyteSource.isFinished()).isTrue();
  }

  @Test
  void testPerStreamWithExtraState() throws Exception {
    // This should never happen but nothing keeps us from processing the reset and not fail
    final List<StreamDescriptor> streamDescriptors = getProtocolStreamDescriptorFromName(Lists.newArrayList("a", "b", "c", "d"));

    final List<StreamDescriptor> streamsToReset = getConfigStreamDescriptorFromName(Lists.newArrayList("a", "b", "c"));

    final ResetSourceConfiguration resetSourceConfiguration = new ResetSourceConfiguration()
        .withStreamsToReset(streamsToReset);
    final WorkerSourceConfig workerSourceConfig = new WorkerSourceConfig()
        .withSourceConnectionConfiguration(Jsons.jsonNode(resetSourceConfiguration))
        .withState(new State()
            .withState(Jsons.jsonNode(createPerStreamState(streamDescriptors))))
        .withCatalog(AIRBYTE_CATALOG);

    emptyAirbyteSource.start(workerSourceConfig, null, null);

    streamsToReset.forEach(s -> testReceiveExpectedPerStreamMessages(s, true));

    Assertions.assertThat(emptyAirbyteSource.attemptRead())
        .isEmpty();

    Assertions.assertThat(emptyAirbyteSource.isFinished()).isTrue();
  }

  @Test
  void testPerStreamWithMissingState() throws Exception {
    final String newStream = "c";

    final List<StreamDescriptor> streamDescriptors = getProtocolStreamDescriptorFromName(Lists.newArrayList("a", "b"));

    final List<StreamDescriptor> streamsToReset = getConfigStreamDescriptorFromName(Lists.newArrayList("a", "b", newStream));

    final ResetSourceConfiguration resetSourceConfiguration = new ResetSourceConfiguration()
        .withStreamsToReset(streamsToReset);
    final WorkerSourceConfig workerSourceConfig = new WorkerSourceConfig()
        .withSourceConnectionConfiguration(Jsons.jsonNode(resetSourceConfiguration))
        .withState(new State()
            .withState(Jsons.jsonNode(createPerStreamState(streamDescriptors))))
        .withCatalog(AIRBYTE_CATALOG);

    emptyAirbyteSource.start(workerSourceConfig, null, null);

    streamsToReset.forEach(s -> testReceiveExpectedPerStreamMessages(s, true));

    Assertions.assertThat(emptyAirbyteSource.attemptRead())
        .isEmpty();

    Assertions.assertThat(emptyAirbyteSource.isFinished()).isTrue();
  }

  // In the LEGACY state, if the list of streams passed in to be reset does not include every stream
  // in the Catalog, then something has gone wrong and we should throw an error
  @Test
  void testLegacyWithMissingCatalogStream() {

    final List<StreamDescriptor> streamsToReset = getConfigStreamDescriptorFromName(Lists.newArrayList("a", "b", "c"));

    final ResetSourceConfiguration resetSourceConfiguration = new ResetSourceConfiguration()
        .withStreamsToReset(streamsToReset);
    final ConfiguredAirbyteCatalog airbyteCatalogWithExtraStream = new ConfiguredAirbyteCatalog()
        .withStreams(Lists.newArrayList(
            new ConfiguredAirbyteStream(getAirbyteStream("a"), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND),
            new ConfiguredAirbyteStream(getAirbyteStream("b"), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND),
            new ConfiguredAirbyteStream(getAirbyteStream("c"), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND),
            new ConfiguredAirbyteStream(getAirbyteStream("d"), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND)));

    final WorkerSourceConfig workerSourceConfig = new WorkerSourceConfig()
        .withSourceConnectionConfiguration(Jsons.jsonNode(resetSourceConfiguration))
        .withState(new State()
            .withState(Jsons.emptyObject()))
        .withCatalog(airbyteCatalogWithExtraStream);

    Assertions.assertThatThrownBy(() -> emptyAirbyteSource.start(workerSourceConfig, null, null))
        .isInstanceOf(IllegalStateException.class);

  }

  // If there are extra streams to reset that do not exist in the Catalog, the reset should work
  // properly with all streams being reset
  @Test
  void testLegacyWithResettingExtraStreamNotInCatalog() throws Exception {
    final List<StreamDescriptor> streamsToResetWithExtra = getConfigStreamDescriptorFromName(Lists.newArrayList("a", "b", "c", "d"));

    final ResetSourceConfiguration resetSourceConfiguration = new ResetSourceConfiguration()
        .withStreamsToReset(streamsToResetWithExtra);
    final ConfiguredAirbyteCatalog airbyteCatalog = new ConfiguredAirbyteCatalog()
        .withStreams(Lists.newArrayList(
            new ConfiguredAirbyteStream(getAirbyteStream("a"), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND),
            new ConfiguredAirbyteStream(getAirbyteStream("b"), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND),
            new ConfiguredAirbyteStream(getAirbyteStream("c"), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND)));

    final WorkerSourceConfig workerSourceConfig = new WorkerSourceConfig()
        .withSourceConnectionConfiguration(Jsons.jsonNode(resetSourceConfiguration))
        .withState(new State()
            .withState(Jsons.jsonNode(Collections.singletonMap("cursor", "1"))))
        .withCatalog(airbyteCatalog);

    emptyAirbyteSource.start(workerSourceConfig, null, null);

    final Optional<AirbyteMessage> maybeMessage = emptyAirbyteSource.attemptRead();
    Assertions.assertThat(maybeMessage)
        .isNotEmpty();

    final AirbyteMessage message = maybeMessage.get();
    Assertions.assertThat(message.getType()).isEqualTo(Type.STATE);

    final AirbyteStateMessage stateMessage = message.getState();
    Assertions.assertThat(stateMessage.getType()).isEqualTo(AirbyteStateType.LEGACY);
    Assertions.assertThat(stateMessage.getData()).isEqualTo(Jsons.emptyObject());

    streamsToResetWithExtra.forEach(this::testReceiveResetMessageTupleForSingleStateTypes);

    Assertions.assertThat(emptyAirbyteSource.attemptRead())
        .isEmpty();

    Assertions.assertThat(emptyAirbyteSource.isFinished()).isTrue();

  }

  @Test
  void testLegacyWithNewConfig() throws Exception {
    final List<StreamDescriptor> streamsToReset = getConfigStreamDescriptorFromName(Lists.newArrayList("a", "b", "c"));

    final ResetSourceConfiguration resetSourceConfiguration = new ResetSourceConfiguration()
        .withStreamsToReset(streamsToReset);
    final ConfiguredAirbyteCatalog airbyteCatalog = new ConfiguredAirbyteCatalog()
        .withStreams(Lists.newArrayList(
            new ConfiguredAirbyteStream(getAirbyteStream("a"), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND),
            new ConfiguredAirbyteStream(getAirbyteStream("b"), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND),
            new ConfiguredAirbyteStream(getAirbyteStream("c"), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND)));

    final WorkerSourceConfig workerSourceConfig = new WorkerSourceConfig()
        .withSourceConnectionConfiguration(Jsons.jsonNode(resetSourceConfiguration))
        .withState(new State()
            .withState(Jsons.jsonNode(Collections.singletonMap("cursor", "1"))))
        .withCatalog(airbyteCatalog);

    emptyAirbyteSource.start(workerSourceConfig, null, null);

    final Optional<AirbyteMessage> maybeMessage = emptyAirbyteSource.attemptRead();
    Assertions.assertThat(maybeMessage)
        .isNotEmpty();

    final AirbyteMessage message = maybeMessage.get();
    Assertions.assertThat(message.getType()).isEqualTo(Type.STATE);

    final AirbyteStateMessage stateMessage = message.getState();
    Assertions.assertThat(stateMessage.getType()).isEqualTo(AirbyteStateType.LEGACY);
    Assertions.assertThat(stateMessage.getData()).isEqualTo(Jsons.emptyObject());

    streamsToReset.forEach(this::testReceiveResetMessageTupleForSingleStateTypes);

    Assertions.assertThat(emptyAirbyteSource.attemptRead())
        .isEmpty();

    Assertions.assertThat(emptyAirbyteSource.isFinished()).isTrue();
  }

  @Test
  void testLegacyWithNewConfigWithCustomFormat() throws Exception {
    emptyAirbyteSource = new EmptyAirbyteSource(true);
    final List<StreamDescriptor> streamsToReset = getConfigStreamDescriptorFromName(Lists.newArrayList("a", "b", "c"));

    final ResetSourceConfiguration resetSourceConfiguration = new ResetSourceConfiguration()
        .withStreamsToReset(streamsToReset);
    final ConfiguredAirbyteCatalog airbyteCatalog = new ConfiguredAirbyteCatalog()
        .withStreams(Lists.newArrayList(
            new ConfiguredAirbyteStream(getAirbyteStream("a"), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND),
            new ConfiguredAirbyteStream(getAirbyteStream("b"), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND),
            new ConfiguredAirbyteStream(getAirbyteStream("c"), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND)));

    final WorkerSourceConfig workerSourceConfig = new WorkerSourceConfig()
        .withSourceConnectionConfiguration(Jsons.jsonNode(resetSourceConfiguration))
        .withState(new State()
            .withState(Jsons.jsonNode(Collections.singletonMap("cursor", "1"))))
        .withCatalog(airbyteCatalog);

    emptyAirbyteSource.start(workerSourceConfig, null, null);

    final Optional<AirbyteMessage> maybeMessage = emptyAirbyteSource.attemptRead();
    Assertions.assertThat(maybeMessage)
        .isNotEmpty();

    final AirbyteMessage message = maybeMessage.get();
    Assertions.assertThat(message.getType()).isEqualTo(Type.STATE);

    final AirbyteStateMessage stateMessage = message.getState();
    Assertions.assertThat(stateMessage.getType()).isEqualTo(AirbyteStateType.LEGACY);
    Assertions.assertThat(stateMessage.getData()).isEqualTo(Jsons.emptyObject());

    Assertions.assertThat(emptyAirbyteSource.attemptRead())
        .isEmpty();

    Assertions.assertThat(emptyAirbyteSource.isFinished()).isTrue();
  }

  @Test
  void testLegacyWithNullState() throws Exception {
    final List<StreamDescriptor> streamsToReset = getConfigStreamDescriptorFromName(Lists.newArrayList("a", "b", "c"));

    final ResetSourceConfiguration resetSourceConfiguration = new ResetSourceConfiguration()
        .withStreamsToReset(streamsToReset);
    final ConfiguredAirbyteCatalog airbyteCatalogWithExtraStream = new ConfiguredAirbyteCatalog()
        .withStreams(Lists.newArrayList(
            new ConfiguredAirbyteStream(getAirbyteStream("a"), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND),
            new ConfiguredAirbyteStream(getAirbyteStream("b"), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND),
            new ConfiguredAirbyteStream(getAirbyteStream("c"), SyncMode.INCREMENTAL, DestinationSyncMode.APPEND)));

    final WorkerSourceConfig workerSourceConfig = new WorkerSourceConfig()
        .withSourceConnectionConfiguration(Jsons.jsonNode(resetSourceConfiguration))
        .withCatalog(airbyteCatalogWithExtraStream);

    emptyAirbyteSource.start(workerSourceConfig, null, null);

    streamsToReset.forEach(this::testReceiveResetMessageTupleForSingleStateTypes);

    final Optional<AirbyteMessage> maybeMessage = emptyAirbyteSource.attemptRead();
    Assertions.assertThat(maybeMessage)
        .isEmpty();

    Assertions.assertThat(emptyAirbyteSource.isFinished()).isTrue();
  }

  private void testReceiveResetStatusMessage(final StreamDescriptor streamDescriptor, final AirbyteStreamStatus status) {
    final Optional<AirbyteMessage> maybeMessage = emptyAirbyteSource.attemptRead();
    Assertions.assertThat(maybeMessage)
        .isNotEmpty();

    final AirbyteMessage message = maybeMessage.get();
    Assertions.assertThat(message.getType()).isEqualTo(Type.TRACE);
    Assertions.assertThat(message.getTrace().getType()).isEqualTo(AirbyteTraceMessage.Type.STREAM_STATUS);
    Assertions.assertThat(message.getTrace().getStreamStatus().getStatus()).isEqualTo(status);

    Assertions.assertThat(message.getTrace().getStreamStatus().getStreamDescriptor()).isEqualTo(
        new io.airbyte.protocol.models.StreamDescriptor()
            .withName(streamDescriptor.getName())
            .withNamespace(streamDescriptor.getNamespace()));
  }

  private void testReceiveNullStreamStateMessage(final StreamDescriptor streamDescriptor) {
    final Optional<AirbyteMessage> maybeMessage = emptyAirbyteSource.attemptRead();
    Assertions.assertThat(maybeMessage)
        .isNotEmpty();

    final AirbyteMessage message = maybeMessage.get();
    Assertions.assertThat(message.getType()).isEqualTo(Type.STATE);

    final AirbyteStateMessage stateMessage = message.getState();
    Assertions.assertThat(stateMessage.getType()).isEqualTo(AirbyteStateType.STREAM);
    Assertions.assertThat(stateMessage.getStream().getStreamDescriptor()).isEqualTo(new io.airbyte.protocol.models.StreamDescriptor()
        .withName(streamDescriptor.getName())
        .withNamespace(streamDescriptor.getNamespace()));
    Assertions.assertThat(stateMessage.getStream().getStreamState()).isNull();
  }

  private void testReceiveExpectedPerStreamMessages(final StreamDescriptor s, final boolean includeStatus) {
    if (includeStatus) {
      testReceiveResetStatusMessage(s, AirbyteStreamStatus.STARTED);
    }
    testReceiveNullStreamStateMessage(s);
    if (includeStatus) {
      testReceiveResetStatusMessage(s, AirbyteStreamStatus.COMPLETE);
    }
  }

  private void testReceiveResetMessageTupleForSingleStateTypes(final StreamDescriptor s) {
    testReceiveResetStatusMessage(s, AirbyteStreamStatus.STARTED);
    testReceiveResetStatusMessage(s, AirbyteStreamStatus.COMPLETE);
  }

  private List<StreamDescriptor> getProtocolStreamDescriptorFromName(final List<String> names) {
    return names.stream().map(
        name -> new StreamDescriptor().withName(name)).toList();
  }

  private List<StreamDescriptor> getConfigStreamDescriptorFromName(final List<String> names) {
    return names.stream().map(
        name -> new StreamDescriptor().withName(name)).toList();
  }

  private void emptyResult() {
    Assertions.assertThat(emptyAirbyteSource.attemptRead())
        .isEmpty();
  }

  private List<AirbyteStateMessage> createPerStreamState(final List<StreamDescriptor> streamDescriptors) {
    return streamDescriptors.stream().map(streamDescriptor -> new AirbyteStateMessage()
        .withType(AirbyteStateType.STREAM)
        .withStream(
            new AirbyteStreamState()
                .withStreamDescriptor(ProtocolConverters.toProtocol(streamDescriptor))
                .withStreamState(Jsons.emptyObject())))
        .toList();
  }

  private List<AirbyteStateMessage> createGlobalState(final List<StreamDescriptor> streamDescriptors, final JsonNode sharedState) {
    final AirbyteGlobalState globalState = new AirbyteGlobalState()
        .withSharedState(sharedState)
        .withStreamStates(
            streamDescriptors.stream().map(streamDescriptor -> new AirbyteStreamState()
                .withStreamDescriptor(ProtocolConverters.toProtocol(streamDescriptor))
                .withStreamState(Jsons.emptyObject()))
                .toList());

    return Lists.newArrayList(
        new AirbyteStateMessage()
            .withType(AirbyteStateType.GLOBAL)
            .withGlobal(globalState));
  }

  @Test
  void emptyLegacyStateShouldNotEmitState() throws Exception {
    final StreamDescriptor streamDescriptor = new StreamDescriptor().withName("test").withNamespace("schema");
    final ResetSourceConfiguration resetSourceConfiguration =
        new ResetSourceConfiguration().withStreamsToReset(Collections.singletonList(streamDescriptor));
    final ConfiguredAirbyteCatalog configuredAirbyteCatalog = new ConfiguredAirbyteCatalog()
        .withStreams(
            Collections.singletonList(new ConfiguredAirbyteStream(
                getAirbyteStream("test").withNamespace("schema"),
                SyncMode.INCREMENTAL,
                DestinationSyncMode.APPEND)));
    final WorkerSourceConfig workerSourceConfig = new WorkerSourceConfig().withSourceId(UUID.randomUUID())
        .withState(new State().withState(Jsons.emptyObject()))
        .withCatalog(configuredAirbyteCatalog)
        .withSourceConnectionConfiguration(Jsons.jsonNode(resetSourceConfiguration));

    emptyAirbyteSource.start(workerSourceConfig, null, null);
    testReceiveResetMessageTupleForSingleStateTypes(streamDescriptor);
    Assertions.assertThat(emptyAirbyteSource.attemptRead())
        .isEmpty();

    Assertions.assertThat(emptyAirbyteSource.isFinished()).isTrue();
  }

  private static AirbyteStream getAirbyteStream(final String name) {
    return new AirbyteStream(name, Jsons.emptyObject(), List.of(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL));
  }

}
