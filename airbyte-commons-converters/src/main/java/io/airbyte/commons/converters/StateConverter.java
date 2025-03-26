/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.converters;

import io.airbyte.api.model.generated.ConnectionState;
import io.airbyte.api.model.generated.ConnectionStateType;
import io.airbyte.api.model.generated.GlobalState;
import io.airbyte.api.model.generated.StreamDescriptor;
import io.airbyte.api.model.generated.StreamState;
import io.airbyte.commons.enums.Enums;
import io.airbyte.config.StateType;
import io.airbyte.config.StateWrapper;
import io.airbyte.protocol.models.AirbyteGlobalState;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.airbyte.protocol.models.AirbyteStateMessage.AirbyteStateType;
import io.airbyte.protocol.models.AirbyteStreamState;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Converters for state.
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
public class StateConverter {

  /**
   * Converts internal representation of state to API representation.
   *
   * @param connectionId connection associated with the state
   * @param stateWrapper internal state representation to convert
   * @return api representation of state
   */
  public static ConnectionState toApi(final UUID connectionId, final @Nullable StateWrapper stateWrapper) {
    return new ConnectionState()
        .connectionId(connectionId)
        .stateType(convertStateTypeToApi(stateWrapper))
        .state(stateWrapper != null ? stateWrapper.getLegacyState() : null)
        .globalState(globalStateToApi(stateWrapper).orElse(null))
        .streamState(streamStateToApi(stateWrapper).orElse(null));
  }

  /**
   * Convert the client model to the API model.
   *
   * @param clientState the client representation
   * @return the API representation
   */
  public static ConnectionState fromClientToApi(final io.airbyte.api.client.model.generated.ConnectionState clientState) {
    return new ConnectionState()
        .connectionId(clientState.getConnectionId())
        .stateType(Enums.convertTo(clientState.getStateType(), ConnectionStateType.class))
        .state(clientState.getState())
        .globalState(clientState.getGlobalState() == null ? null : globalStateFromClientToApi(clientState.getGlobalState()))
        .streamState(clientState.getStreamState() == null ? null
            : clientState.getStreamState().stream().map(StateConverter::streamStateFromClientToApi).toList());

  }

  private static GlobalState globalStateFromClientToApi(io.airbyte.api.client.model.generated.GlobalState clientGlobalState) {
    return new GlobalState()
        .sharedState(clientGlobalState.getSharedState())
        .streamStates(clientGlobalState.getStreamStates().stream().map(StateConverter::streamStateFromClientToApi).toList());
  }

  private static StreamState streamStateFromClientToApi(io.airbyte.api.client.model.generated.StreamState clientStreamState) {
    return new StreamState()
        .streamDescriptor(new StreamDescriptor()
            .name(clientStreamState.getStreamDescriptor().getName())
            .namespace(clientStreamState.getStreamDescriptor().getNamespace()))
        .streamState(clientStreamState.getStreamState());
  }

  /**
   * Converts internal representation of state to client representation.
   *
   * @param connectionId connection associated with the state
   * @param stateWrapper internal state representation to convert
   * @return client representation of state
   */
  public static io.airbyte.api.client.model.generated.ConnectionState toClient(final UUID connectionId, final @Nullable StateWrapper stateWrapper) {
    return new io.airbyte.api.client.model.generated.ConnectionState(
        convertStateTypeToClient(stateWrapper),
        connectionId,
        stateWrapper != null ? stateWrapper.getLegacyState() : null,
        streamStateToClient(stateWrapper).orElse(null),
        globalStateToClient(stateWrapper).orElse(null));
  }

  /**
   * Converts API representation of state to internal representation.
   *
   * @param apiConnectionState api representation of state
   * @return internal representation of state
   */
  public static StateWrapper toInternal(final @Nullable ConnectionState apiConnectionState) {
    return new StateWrapper()
        .withStateType(convertStateTypeToInternal(apiConnectionState).orElse(null))
        .withGlobal(globalStateToInternal(apiConnectionState).orElse(null))
        .withLegacyState(apiConnectionState != null ? apiConnectionState.getState() : null)
        .withStateMessages(streamStateToInternal(apiConnectionState).orElse(null));

  }

  /**
   * Api connection state to platform state representation.
   *
   * @param clientConnectionState api client state
   * @return platform state representation
   */
  public static StateWrapper clientToInternal(final @Nullable io.airbyte.api.client.model.generated.ConnectionState clientConnectionState) {
    return new StateWrapper()
        .withStateType(clientConnectionState != null ? convertClientStateTypeToInternal(clientConnectionState.getStateType()) : null)
        .withGlobal(clientGlobalStateToInternal(clientConnectionState).orElse(null))
        .withLegacyState(clientConnectionState != null ? clientConnectionState.getState() : null)
        .withStateMessages(clientStreamStateToInternal(clientConnectionState).orElse(null));

  }

  /**
   * Convert api connection state type to internal model.
   *
   * @param connectionStateType api state type
   * @return internal state type
   */
  @SuppressWarnings("LineLength")
  public static StateType convertClientStateTypeToInternal(final @Nullable io.airbyte.api.client.model.generated.ConnectionStateType connectionStateType) {
    if (connectionStateType == null || connectionStateType.equals(io.airbyte.api.client.model.generated.ConnectionStateType.NOT_SET)) {
      return null;
    } else {
      return Enums.convertTo(connectionStateType, StateType.class);
    }
  }

  /**
   * Convert to API representation of state type. API has an additional type (NOT_SET). This
   * represents the case where no state is saved so we do not know the state type.
   *
   * @param stateWrapper state to convert
   * @return api representation of state type
   */
  private static ConnectionStateType convertStateTypeToApi(final @Nullable StateWrapper stateWrapper) {
    if (stateWrapper == null || stateWrapper.getStateType() == null) {
      return ConnectionStateType.NOT_SET;
    } else {
      return Enums.convertTo(stateWrapper.getStateType(), ConnectionStateType.class);
    }
  }

  /**
   * Convert to client representation of state type. The client model has an additional type
   * (NOT_SET). This represents the case where no state is saved so we do not know the state type.
   *
   * @param stateWrapper state to convert
   * @return client representation of state type
   */
  private static io.airbyte.api.client.model.generated.ConnectionStateType convertStateTypeToClient(final @Nullable StateWrapper stateWrapper) {
    if (stateWrapper == null || stateWrapper.getStateType() == null) {
      return io.airbyte.api.client.model.generated.ConnectionStateType.NOT_SET;
    } else {
      return Enums.convertTo(stateWrapper.getStateType(), io.airbyte.api.client.model.generated.ConnectionStateType.class);
    }
  }

  /**
   * Convert to internal representation of state type, if set. Otherise, empty optional
   *
   * @param connectionState API state to convert.
   * @return internal state type, if set. Otherwise, empty optional.
   */
  private static Optional<StateType> convertStateTypeToInternal(final @Nullable ConnectionState connectionState) {
    if (connectionState == null || connectionState.getStateType().equals(ConnectionStateType.NOT_SET)) {
      return Optional.empty();
    } else {
      return Optional.of(Enums.convertTo(connectionState.getStateType(), StateType.class));
    }
  }

  /**
   * If wrapper is of type global state, returns API representation of global state. Otherwise, empty
   * optional.
   *
   * @param stateWrapper state wrapper to extract from
   * @return api representation of global state if state wrapper is type global. Otherwise, empty
   *         optional.
   */
  private static Optional<GlobalState> globalStateToApi(final @Nullable StateWrapper stateWrapper) {
    if (stateWrapper != null
        && stateWrapper.getStateType() == StateType.GLOBAL
        && stateWrapper.getGlobal() != null
        && stateWrapper.getGlobal().getGlobal() != null) {
      return Optional.of(new GlobalState()
          .sharedState(stateWrapper.getGlobal().getGlobal().getSharedState())
          .streamStates(stateWrapper.getGlobal().getGlobal().getStreamStates()
              .stream()
              .map(StateConverter::streamStateStructToApi)
              .toList()));
    } else {
      return Optional.empty();
    }
  }

  /**
   * If wrapper is of type global state, returns client representation of global state. Otherwise,
   * empty optional.
   *
   * @param stateWrapper state wrapper to extract from
   * @return client representation of global state if state wrapper is type global. Otherwise, empty
   *         optional.
   */
  private static Optional<io.airbyte.api.client.model.generated.GlobalState> globalStateToClient(final @Nullable StateWrapper stateWrapper) {
    if (stateWrapper != null
        && stateWrapper.getStateType() == StateType.GLOBAL
        && stateWrapper.getGlobal() != null
        && stateWrapper.getGlobal().getGlobal() != null) {
      return Optional.of(new io.airbyte.api.client.model.generated.GlobalState(
          stateWrapper.getGlobal().getGlobal().getStreamStates()
              .stream()
              .map(StateConverter::streamStateStructToClient)
              .toList(),
          stateWrapper.getGlobal().getGlobal().getSharedState()));
    } else {
      return Optional.empty();
    }
  }

  /**
   * If API state is of type global, returns internal representation of global state. Otherwise, empty
   * optional.
   *
   * @param connectionState API state representation to extract from
   * @return global state message if API state is of type global. Otherwise, empty optional.
   */
  private static Optional<AirbyteStateMessage> globalStateToInternal(final @Nullable ConnectionState connectionState) {
    if (connectionState != null
        && connectionState.getStateType() == ConnectionStateType.GLOBAL
        && connectionState.getGlobalState() != null) {
      return Optional.of(new AirbyteStateMessage()
          .withType(AirbyteStateType.GLOBAL)
          .withGlobal(new AirbyteGlobalState()
              .withSharedState(connectionState.getGlobalState().getSharedState())
              .withStreamStates(connectionState.getGlobalState().getStreamStates()
                  .stream()
                  .map(StateConverter::streamStateStructToInternal)
                  .toList())));
    } else {
      return Optional.empty();
    }
  }

  @SuppressWarnings("LineLength")
  private static Optional<AirbyteStateMessage> clientGlobalStateToInternal(final @Nullable io.airbyte.api.client.model.generated.ConnectionState connectionState) {
    if (connectionState != null
        && connectionState.getStateType() == io.airbyte.api.client.model.generated.ConnectionStateType.GLOBAL
        && connectionState.getGlobalState() != null) {
      return Optional.of(new AirbyteStateMessage()
          .withType(AirbyteStateType.GLOBAL)
          .withGlobal(new AirbyteGlobalState()
              .withSharedState(connectionState.getGlobalState().getSharedState())
              .withStreamStates(connectionState.getGlobalState().getStreamStates()
                  .stream()
                  .map(StateConverter::clientStreamStateStructToInternal)
                  .toList())));
    } else {
      return Optional.empty();
    }
  }

  /**
   * If wrapper is of type stream state, returns API representation of stream state. Otherwise, empty
   * optional.
   *
   * @param stateWrapper state wrapper to extract from
   * @return api representation of stream state if state wrapper is type stream. Otherwise, empty
   *         optional.
   */
  private static Optional<List<StreamState>> streamStateToApi(final @Nullable StateWrapper stateWrapper) {
    if (stateWrapper != null && stateWrapper.getStateType() == StateType.STREAM && stateWrapper.getStateMessages() != null) {
      return Optional.ofNullable(stateWrapper.getStateMessages()
          .stream()
          .map(AirbyteStateMessage::getStream)
          .map(StateConverter::streamStateStructToApi)
          .toList());
    } else {
      return Optional.empty();
    }
  }

  /**
   * If wrapper is of type stream state, returns client representation of stream state. Otherwise,
   * empty optional.
   *
   * @param stateWrapper state wrapper to extract from
   * @return client representation of stream state if state wrapper is type stream. Otherwise, empty
   *         optional.
   */
  private static Optional<List<io.airbyte.api.client.model.generated.StreamState>> streamStateToClient(final @Nullable StateWrapper stateWrapper) {
    if (stateWrapper != null && stateWrapper.getStateType() == StateType.STREAM && stateWrapper.getStateMessages() != null) {
      return Optional.ofNullable(stateWrapper.getStateMessages()
          .stream()
          .map(AirbyteStateMessage::getStream)
          .map(StateConverter::streamStateStructToClient)
          .toList());
    } else {
      return Optional.empty();
    }
  }

  /**
   * If API state is of type stream, returns internal representation of stream state. Otherwise, empty
   * optional.
   *
   * @param connectionState API representation of state to extract from
   * @return internal representation of stream state if API state representation is of type stream.
   *         Otherwise, empty optional.
   */
  private static Optional<List<AirbyteStateMessage>> streamStateToInternal(final @Nullable ConnectionState connectionState) {
    if (connectionState != null && connectionState.getStateType() == ConnectionStateType.STREAM && connectionState.getStreamState() != null) {
      return Optional.ofNullable(connectionState.getStreamState()
          .stream()
          .map(StateConverter::streamStateStructToInternal)
          .map(s -> new AirbyteStateMessage().withType(AirbyteStateType.STREAM).withStream(s))
          .toList());
    } else {
      return Optional.empty();
    }
  }

  @SuppressWarnings("LineLength")
  private static Optional<List<AirbyteStateMessage>> clientStreamStateToInternal(final @Nullable io.airbyte.api.client.model.generated.ConnectionState connectionState) {
    if (connectionState != null && connectionState.getStateType() == io.airbyte.api.client.model.generated.ConnectionStateType.STREAM
        && connectionState.getStreamState() != null) {
      return Optional.ofNullable(connectionState.getStreamState()
          .stream()
          .map(StateConverter::clientStreamStateStructToInternal)
          .map(s -> new AirbyteStateMessage().withType(AirbyteStateType.STREAM).withStream(s))
          .toList());
    } else {
      return Optional.empty();
    }
  }

  private static StreamState streamStateStructToApi(final AirbyteStreamState streamState) {
    return new StreamState()
        .streamDescriptor(streamDescriptorToApi(streamState.getStreamDescriptor()))
        .streamState(streamState.getStreamState());
  }

  private static io.airbyte.api.client.model.generated.StreamState streamStateStructToClient(final AirbyteStreamState streamState) {
    return new io.airbyte.api.client.model.generated.StreamState(
        streamDescriptorToClient(streamState.getStreamDescriptor()),
        streamState.getStreamState());
  }

  private static AirbyteStreamState streamStateStructToInternal(final StreamState streamState) {
    return new AirbyteStreamState()
        .withStreamDescriptor(streamDescriptorToProtocol(streamState.getStreamDescriptor()))
        .withStreamState(streamState.getStreamState());
  }

  private static AirbyteStreamState clientStreamStateStructToInternal(final io.airbyte.api.client.model.generated.StreamState streamState) {
    return new AirbyteStreamState()
        .withStreamDescriptor(clientStreamDescriptorToProtocol(streamState.getStreamDescriptor()))
        .withStreamState(streamState.getStreamState());
  }

  // The conversions methods below are internal to convert from protocol to API client.
  // Eventually, we should be using config.StreamDescriptor internally and using ApiClientConverters
  // instead.
  // Keeping those private until this is fixed.
  private static StreamDescriptor streamDescriptorToApi(final io.airbyte.protocol.models.StreamDescriptor protocolStreamDescriptor) {
    return new StreamDescriptor().name(protocolStreamDescriptor.getName()).namespace(protocolStreamDescriptor.getNamespace());
  }

  @SuppressWarnings("LineLength")
  private static io.airbyte.api.client.model.generated.StreamDescriptor streamDescriptorToClient(final io.airbyte.protocol.models.StreamDescriptor protocolStreamDescriptor) {
    return new io.airbyte.api.client.model.generated.StreamDescriptor(protocolStreamDescriptor.getName(), protocolStreamDescriptor.getNamespace());
  }

  private static io.airbyte.protocol.models.StreamDescriptor streamDescriptorToProtocol(final StreamDescriptor apiStreamDescriptor) {
    return new io.airbyte.protocol.models.StreamDescriptor().withName(apiStreamDescriptor.getName())
        .withNamespace(apiStreamDescriptor.getNamespace());
  }

  @SuppressWarnings("LineLength")
  private static io.airbyte.protocol.models.StreamDescriptor clientStreamDescriptorToProtocol(final io.airbyte.api.client.model.generated.StreamDescriptor clientStreamDescriptor) {
    return new io.airbyte.protocol.models.StreamDescriptor().withName(clientStreamDescriptor.getName())
        .withNamespace(clientStreamDescriptor.getNamespace());
  }

}
