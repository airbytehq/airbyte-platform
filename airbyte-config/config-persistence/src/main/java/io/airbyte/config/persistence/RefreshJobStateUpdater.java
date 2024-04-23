/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import io.airbyte.config.StateType;
import io.airbyte.config.StateWrapper;
import io.airbyte.config.persistence.domain.StreamRefresh;
import io.airbyte.protocol.models.AirbyteGlobalState;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.airbyte.protocol.models.AirbyteStreamState;
import io.airbyte.protocol.models.StreamDescriptor;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Singleton
public class RefreshJobStateUpdater {

  private final StatePersistence statePersistence;

  public RefreshJobStateUpdater(final StatePersistence statePersistence) {
    this.statePersistence = statePersistence;
  }

  public void updateStateWrapperForRefresh(final UUID connectionId, final StateWrapper currentState, final List<StreamRefresh> streamsToRefresh)
      throws IOException {
    final StateWrapper updatedState = new StateWrapper();
    final Set<StreamDescriptor> streamDescriptorsToRefresh = streamsToRefresh
        .stream()
        .map(c -> new StreamDescriptor().withName(c.getStreamName()).withNamespace(c.getStreamNamespace()))
        .collect(Collectors.toSet());

    switch (currentState.getStateType()) {
      case GLOBAL -> {
        final List<AirbyteStreamState> streamStatesToRetain = new ArrayList<>();
        final AirbyteStateMessage currentGlobalStateMessage = currentState.getGlobal();
        final List<AirbyteStreamState> currentStreamStates = currentGlobalStateMessage.getGlobal().getStreamStates();
        for (final AirbyteStreamState streamState : currentStreamStates) {
          final StreamDescriptor streamDescriptor = streamState.getStreamDescriptor();
          if (!streamDescriptorsToRefresh.contains(streamDescriptor)) {
            streamStatesToRetain.add(streamState);
          }
        }
        updatedState.setStateType(StateType.GLOBAL);
        updatedState.setGlobal(new AirbyteStateMessage()
            .withType(AirbyteStateMessage.AirbyteStateType.GLOBAL)
            .withGlobal(new AirbyteGlobalState()
                .withSharedState(streamStatesToRetain.isEmpty() ? null : currentGlobalStateMessage.getGlobal().getSharedState())
                .withStreamStates(streamStatesToRetain)));

      }
      case STREAM -> {
        final List<AirbyteStateMessage> streamStatesToRetain = new ArrayList<>();
        for (final AirbyteStateMessage stateMessage : currentState.getStateMessages()) {
          final StreamDescriptor streamDescriptor = stateMessage.getStream().getStreamDescriptor();
          if (!streamDescriptorsToRefresh.contains(streamDescriptor)) {
            streamStatesToRetain.add(stateMessage);
          }
        }
        updatedState.setStateType(StateType.STREAM);
        updatedState.setStateMessages(streamStatesToRetain);
      }
      default -> updatedState.setStateType(StateType.LEGACY);
    }
    statePersistence.updateOrCreateState(connectionId, updatedState);
  }

}
