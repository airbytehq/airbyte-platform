/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.StateType;
import io.airbyte.config.StateWrapper;
import io.airbyte.config.persistence.domain.StreamRefresh;
import io.airbyte.protocol.models.AirbyteGlobalState;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.airbyte.protocol.models.AirbyteStreamState;
import io.airbyte.protocol.models.StreamDescriptor;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RefreshJobStateUpdaterTest {

  private StatePersistence statePersistence;
  private RefreshJobStateUpdater refreshJobStateUpdater;

  @BeforeEach
  public void init() {
    statePersistence = mock(StatePersistence.class);
    refreshJobStateUpdater = new RefreshJobStateUpdater(statePersistence);
  }

  @Test
  public void streamStateTest() throws IOException {
    final UUID connectionId = UUID.randomUUID();
    final String streamToRefresh = "name";
    final String streamToNotRefresh = "stream-not-refresh";
    final String streamNamespace = "namespace";
    final AirbyteStateMessage stateMessageFromRefreshStream = new AirbyteStateMessage()
        .withType(AirbyteStateMessage.AirbyteStateType.STREAM)
        .withStream(new AirbyteStreamState()
            .withStreamDescriptor(new StreamDescriptor().withName(streamToRefresh).withNamespace(streamNamespace))
            .withStreamState(Jsons.jsonNode(ImmutableMap.of("cursor", 1))));

    final AirbyteStateMessage stateMessageFromNonRefreshStream = new AirbyteStateMessage()
        .withType(AirbyteStateMessage.AirbyteStateType.STREAM)
        .withStream(new AirbyteStreamState()
            .withStreamDescriptor(new StreamDescriptor().withName(streamToNotRefresh).withNamespace(streamNamespace))
            .withStreamState(Jsons.jsonNode(ImmutableMap.of("cursor-2", 2))));

    final StateWrapper stateWrapper = new StateWrapper().withStateType(StateType.STREAM)
        .withStateMessages(Arrays.asList(stateMessageFromRefreshStream, stateMessageFromNonRefreshStream));

    refreshJobStateUpdater.updateStateWrapperForRefresh(connectionId, stateWrapper,
        List.of(new StreamRefresh(UUID.randomUUID(), connectionId, streamToRefresh, streamNamespace, null)));
    final StateWrapper expected =
        new StateWrapper().withStateType(StateType.STREAM).withStateMessages(Collections.singletonList(stateMessageFromNonRefreshStream));
    verify(statePersistence).updateOrCreateState(connectionId, expected);
  }

  @Test
  public void globalStateTest() throws IOException {
    final UUID connectionId = UUID.randomUUID();
    final String streamToRefresh = "name";
    final String streamToNotRefresh = "stream-not-refresh";
    final String streamNamespace = "namespace";
    final AirbyteStreamState stateMessageFromRefreshStream = new AirbyteStreamState()
        .withStreamDescriptor(new StreamDescriptor().withName(streamToRefresh).withNamespace(streamNamespace))
        .withStreamState(Jsons.jsonNode(ImmutableMap.of("cursor", 1)));

    final AirbyteStreamState stateMessageFromNonRefreshStream = new AirbyteStreamState()
        .withStreamDescriptor(new StreamDescriptor().withName(streamToNotRefresh).withNamespace(streamNamespace))
        .withStreamState(Jsons.jsonNode(ImmutableMap.of("cursor-2", 2)));

    final JsonNode sharedState = Jsons.jsonNode(ImmutableMap.of("shared-state", 5));
    final AirbyteStateMessage existingStateMessage = new AirbyteStateMessage()
        .withType(AirbyteStateMessage.AirbyteStateType.GLOBAL)
        .withGlobal(new AirbyteGlobalState().withSharedState(sharedState)
            .withStreamStates(Arrays.asList(stateMessageFromRefreshStream, stateMessageFromNonRefreshStream)));

    final StateWrapper stateWrapper = new StateWrapper().withStateType(StateType.GLOBAL).withGlobal(existingStateMessage);

    final AirbyteStateMessage expectedStateMessage = new AirbyteStateMessage()
        .withType(AirbyteStateMessage.AirbyteStateType.GLOBAL)
        .withGlobal(
            new AirbyteGlobalState().withSharedState(sharedState).withStreamStates(Collections.singletonList(stateMessageFromNonRefreshStream)));

    refreshJobStateUpdater.updateStateWrapperForRefresh(connectionId, stateWrapper,
        List.of(new StreamRefresh(UUID.randomUUID(), connectionId, streamToRefresh, streamNamespace, null)));

    final StateWrapper expected = new StateWrapper().withStateType(StateType.GLOBAL).withGlobal(expectedStateMessage);
    verify(statePersistence).updateOrCreateState(connectionId, expected);
  }

  @Test
  public void fullGlobalState() throws IOException {
    final UUID connectionId = UUID.randomUUID();
    final String streamToRefresh = "name";
    final String streamToRefresh2 = "stream-refresh2";
    final String streamNamespace = "namespace";

    final AirbyteStreamState stateMessageFromRefreshStream = new AirbyteStreamState()
        .withStreamDescriptor(new StreamDescriptor().withName(streamToRefresh).withNamespace(streamNamespace))
        .withStreamState(Jsons.jsonNode(ImmutableMap.of("cursor", 1)));

    final AirbyteStreamState stateMessageFromNonRefreshStream = new AirbyteStreamState()
        .withStreamDescriptor(new StreamDescriptor().withName(streamToRefresh2).withNamespace(streamNamespace))
        .withStreamState(Jsons.jsonNode(ImmutableMap.of("cursor-2", 2)));

    final JsonNode sharedState = Jsons.jsonNode(ImmutableMap.of("shared-state", 5));

    final AirbyteStateMessage existingStateMessage = new AirbyteStateMessage()
        .withType(AirbyteStateMessage.AirbyteStateType.GLOBAL)
        .withGlobal(new AirbyteGlobalState().withSharedState(sharedState)
            .withStreamStates(Arrays.asList(stateMessageFromRefreshStream, stateMessageFromNonRefreshStream)));

    final StateWrapper stateWrapper = new StateWrapper().withStateType(StateType.GLOBAL).withGlobal(existingStateMessage);

    refreshJobStateUpdater.updateStateWrapperForRefresh(connectionId, stateWrapper,
        List.of(new StreamRefresh(UUID.randomUUID(), connectionId, streamToRefresh, streamNamespace, null),
            new StreamRefresh(UUID.randomUUID(), connectionId, streamToRefresh2, streamNamespace, null)));
    final AirbyteStateMessage expectedStateMessage = new AirbyteStateMessage()
        .withType(AirbyteStateMessage.AirbyteStateType.GLOBAL)
        .withGlobal(new AirbyteGlobalState().withSharedState(null).withStreamStates(Collections.emptyList()));

    final StateWrapper expected = new StateWrapper().withStateType(StateType.GLOBAL).withGlobal(expectedStateMessage);
    verify(statePersistence).updateOrCreateState(connectionId, expected);
  }

}
