/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.converters

import io.airbyte.api.model.generated.ConnectionState
import io.airbyte.api.model.generated.ConnectionStateType
import io.airbyte.api.model.generated.GlobalState
import io.airbyte.api.model.generated.StreamDescriptor
import io.airbyte.api.model.generated.StreamState
import io.airbyte.commons.enums.convertTo
import io.airbyte.config.StateType
import io.airbyte.config.StateWrapper
import io.airbyte.protocol.models.v0.AirbyteGlobalState
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStateMessage.AirbyteStateType
import io.airbyte.protocol.models.v0.AirbyteStreamState
import java.util.UUID
import io.airbyte.api.client.model.generated.ConnectionState as ClientConnectionState
import io.airbyte.api.client.model.generated.ConnectionStateType as ClientConnectionStateType
import io.airbyte.api.client.model.generated.GlobalState as ClientGlobalState
import io.airbyte.api.client.model.generated.StreamDescriptor as ClientStreamDescriptor
import io.airbyte.api.client.model.generated.StreamState as ClientStreamState
import io.airbyte.protocol.models.v0.StreamDescriptor as ProtocolStreamDescriptor

/**
 * Converters for state.
 */
object StateConverter {
  /**
   * Converts internal representation of state to API representation.
   *
   * @param connectionId connection associated with the state
   * @param stateWrapper internal state representation to convert
   * @return api representation of state
   */
  @JvmStatic
  fun toApi(
    connectionId: UUID,
    stateWrapper: StateWrapper?,
  ): ConnectionState =
    ConnectionState()
      .connectionId(connectionId)
      .stateType(convertStateTypeToApi(stateWrapper))
      .state(stateWrapper?.legacyState)
      .globalState(globalStateToApi(stateWrapper))
      .streamState(streamStateToApi(stateWrapper))

  /**
   * Convert the client model to the API model.
   *
   * @param clientState the client representation
   * @return the API representation
   */
  @JvmStatic
  fun fromClientToApi(clientState: ClientConnectionState): ConnectionState =
    ConnectionState()
      .connectionId(clientState.connectionId)
      .stateType(clientState.stateType.convertTo<ConnectionStateType>())
      .state(clientState.state)
      .globalState(clientState.globalState?.let { globalStateFromClientToApi(it) })
      .streamState(
        clientState.streamState
          ?.map { streamStateFromClientToApi(it) }
          ?.toList(),
      )

  /**
   * Converts internal representation of state to client representation.
   *
   * @param connectionId connection associated with the state
   * @param stateWrapper internal state representation to convert
   * @return client representation of state
   */
  @JvmStatic
  fun toClient(
    connectionId: UUID,
    stateWrapper: StateWrapper?,
  ): ClientConnectionState =
    ClientConnectionState(
      convertStateTypeToClient(stateWrapper),
      connectionId,
      stateWrapper?.legacyState,
      streamStateToClient(stateWrapper),
      globalStateToClient(stateWrapper),
    )

  /**
   * Converts API representation of state to internal representation.
   *
   * @param apiConnectionState api representation of state
   * @return internal representation of state
   */
  @JvmStatic
  fun toInternal(apiConnectionState: ConnectionState?): StateWrapper =
    StateWrapper()
      .withStateType(convertStateTypeToInternal(apiConnectionState))
      .withGlobal(globalStateToInternal(apiConnectionState))
      .withLegacyState(apiConnectionState?.state)
      .withStateMessages(streamStateToInternal(apiConnectionState))

  /**
   * Api connection state to platform state representation.
   *
   * @param clientConnectionState api client state
   * @return platform state representation
   */
  @JvmStatic
  fun clientToInternal(clientConnectionState: ClientConnectionState?): StateWrapper =
    StateWrapper()
      .withStateType(clientConnectionState?.let { convertClientStateTypeToInternal(it.stateType) })
      .withGlobal(clientGlobalStateToInternal(clientConnectionState))
      .withLegacyState(clientConnectionState?.state)
      .withStateMessages(clientStreamStateToInternal(clientConnectionState))
}

/**
 * Convert api connection state type to internal model.
 *
 * @param connectionStateType api state type
 * @return internal state type
 */
private fun convertClientStateTypeToInternal(connectionStateType: ClientConnectionStateType?): StateType? =
  if (connectionStateType == null || connectionStateType == ClientConnectionStateType.NOT_SET) {
    null
  } else {
    connectionStateType.convertTo<StateType>()
  }

/**
 * Convert to API representation of state type. API has an additional type (NOT_SET). This
 * represents the case where no state is saved so we do not know the state type.
 *
 * @param stateWrapper state to convert
 * @return api representation of state type
 */
private fun convertStateTypeToApi(stateWrapper: StateWrapper?): ConnectionStateType? =
  if (stateWrapper?.stateType == null) {
    ConnectionStateType.NOT_SET
  } else {
    stateWrapper.stateType.convertTo<ConnectionStateType>()
  }

/**
 * Convert to client representation of state type. The client model has an additional type
 * (NOT_SET). This represents the case where no state is saved so we do not know the state type.
 *
 * @param stateWrapper state to convert
 * @return client representation of state type
 */
private fun convertStateTypeToClient(stateWrapper: StateWrapper?): ClientConnectionStateType =
  stateWrapper?.stateType?.let { it.convertTo<ClientConnectionStateType>() } ?: ClientConnectionStateType.NOT_SET

/**
 * Convert to internal representation of state type, if set. Otherise, empty optional
 *
 * @param connectionState API state to convert.
 * @return internal state type, if set. Otherwise, empty optional.
 */
private fun convertStateTypeToInternal(connectionState: ConnectionState?): StateType? =
  if (connectionState == null || connectionState.stateType == ConnectionStateType.NOT_SET) {
    null
  } else {
    connectionState.stateType.convertTo<StateType>()
  }

private fun globalStateFromClientToApi(clientGlobalState: ClientGlobalState): GlobalState =
  GlobalState()
    .sharedState(clientGlobalState.sharedState)
    .streamStates(clientGlobalState.streamStates.map { streamStateFromClientToApi(it) })

private fun streamStateFromClientToApi(clientStreamState: ClientStreamState): StreamState =
  StreamState()
    .streamDescriptor(
      StreamDescriptor()
        .name(clientStreamState.streamDescriptor.name)
        .namespace(clientStreamState.streamDescriptor.namespace),
    ).streamState(clientStreamState.streamState)

/**
 * If wrapper is of type global state, returns API representation of global state. Otherwise, empty
 * optional.
 *
 * @param stateWrapper state wrapper to extract from
 * @return api representation of global state if state wrapper is type global. Otherwise, empty
 * optional.
 */
private fun globalStateToApi(stateWrapper: StateWrapper?): GlobalState? =
  if (stateWrapper != null &&
    stateWrapper.stateType == StateType.GLOBAL &&
    stateWrapper.global != null &&
    stateWrapper.global.global != null
  ) {
    GlobalState()
      .sharedState(stateWrapper.global.global.sharedState)
      .streamStates(
        stateWrapper.global.global.streamStates
          .map { streamStateStructToApi(it) },
      )
  } else {
    null
  }

/**
 * If wrapper is of type global state, returns client representation of global state. Otherwise,
 * empty optional.
 *
 * @param stateWrapper state wrapper to extract from
 * @return client representation of global state if state wrapper is type global. Otherwise, empty
 * optional.
 */
private fun globalStateToClient(stateWrapper: StateWrapper?): ClientGlobalState? =
  if (stateWrapper != null &&
    stateWrapper.stateType == StateType.GLOBAL &&
    stateWrapper.global != null &&
    stateWrapper.global.global != null
  ) {
    ClientGlobalState(
      stateWrapper.global.global.streamStates
        .map { streamStateStructToClient(it) },
      stateWrapper.global.global.sharedState,
    )
  } else {
    null
  }

/**
 * If API state is of type global, returns internal representation of global state. Otherwise, empty
 * optional.
 *
 * @param connectionState API state representation to extract from
 * @return global state message if API state is of type global. Otherwise, empty optional.
 */
private fun globalStateToInternal(connectionState: ConnectionState?): AirbyteStateMessage? =
  if (connectionState != null &&
    connectionState.stateType == ConnectionStateType.GLOBAL &&
    connectionState.globalState != null
  ) {
    AirbyteStateMessage()
      .withType(AirbyteStateType.GLOBAL)
      .withGlobal(
        AirbyteGlobalState()
          .withSharedState(connectionState.globalState.sharedState)
          .withStreamStates(
            connectionState.globalState.streamStates.map {
              streamStateStructToInternal(it)
            },
          ),
      )
  } else {
    null
  }

private fun clientGlobalStateToInternal(connectionState: ClientConnectionState?): AirbyteStateMessage? =
  if (connectionState != null &&
    connectionState.stateType == ClientConnectionStateType.GLOBAL &&
    connectionState.globalState != null
  ) {
    AirbyteStateMessage()
      .withType(AirbyteStateType.GLOBAL)
      .withGlobal(
        AirbyteGlobalState()
          .withSharedState(connectionState.globalState?.sharedState)
          .withStreamStates(
            connectionState.globalState?.streamStates?.map {
              clientStreamStateStructToInternal(it)
            },
          ),
      )
  } else {
    null
  }

/**
 * If wrapper is of type stream state, returns API representation of stream state. Otherwise, empty
 * optional.
 *
 * @param stateWrapper state wrapper to extract from
 * @return api representation of stream state if state wrapper is type stream. Otherwise, empty
 * optional.
 */
private fun streamStateToApi(stateWrapper: StateWrapper?): List<StreamState>? =
  if (stateWrapper != null && stateWrapper.stateType == StateType.STREAM && stateWrapper.stateMessages != null) {
    stateWrapper.stateMessages.map { streamStateStructToApi(it.stream) }
  } else {
    null
  }

/**
 * If wrapper is of type stream state, returns client representation of stream state. Otherwise,
 * empty optional.
 *
 * @param stateWrapper state wrapper to extract from
 * @return client representation of stream state if state wrapper is type stream. Otherwise, empty
 * optional.
 */
private fun streamStateToClient(stateWrapper: StateWrapper?): List<ClientStreamState>? =
  if (stateWrapper != null && stateWrapper.stateType == StateType.STREAM && stateWrapper.stateMessages != null) {
    stateWrapper.stateMessages.map { streamStateStructToClient(it.stream) }
  } else {
    null
  }

/**
 * If API state is of type stream, returns internal representation of stream state. Otherwise, empty
 * optional.
 *
 * @param connectionState API representation of state to extract from
 * @return internal representation of stream state if API state representation is of type stream.
 * Otherwise, empty optional.
 */
private fun streamStateToInternal(connectionState: ConnectionState?): List<AirbyteStateMessage>? =
  if (connectionState != null &&
    connectionState.stateType == ConnectionStateType.STREAM &&
    connectionState.streamState != null
  ) {
    connectionState.streamState.map {
      AirbyteStateMessage()
        .withType(AirbyteStateType.STREAM)
        .withStream(streamStateStructToInternal(it))
    }
  } else {
    null
  }

private fun clientStreamStateToInternal(connectionState: ClientConnectionState?): List<AirbyteStateMessage>? =
  if (connectionState?.stateType == ClientConnectionStateType.STREAM) {
    connectionState.streamState?.map {
      AirbyteStateMessage()
        .withType(AirbyteStateType.STREAM)
        .withStream(clientStreamStateStructToInternal(it))
    }
  } else {
    null
  }

private fun streamStateStructToApi(streamState: AirbyteStreamState): StreamState =
  StreamState()
    .streamDescriptor(streamDescriptorToApi(streamState.streamDescriptor))
    .streamState(streamState.streamState)

private fun streamStateStructToClient(streamState: AirbyteStreamState): ClientStreamState =
  io.airbyte.api.client.model.generated.StreamState(
    streamDescriptorToClient(streamState.streamDescriptor),
    streamState.streamState,
  )

private fun streamStateStructToInternal(streamState: StreamState): AirbyteStreamState =
  AirbyteStreamState()
    .withStreamDescriptor(streamDescriptorToProtocol(streamState.streamDescriptor))
    .withStreamState(streamState.streamState)

private fun clientStreamStateStructToInternal(streamState: ClientStreamState): AirbyteStreamState =
  AirbyteStreamState()
    .withStreamDescriptor(clientStreamDescriptorToProtocol(streamState.streamDescriptor))
    .withStreamState(streamState.streamState)

// The conversions methods below are internal to convert from protocol to API client.
// Eventually, we should be using config.StreamDescriptor internally and using ApiClientConverters instead.
// Keeping those private until this is fixed.
private fun streamDescriptorToApi(protocolStreamDescriptor: ProtocolStreamDescriptor): StreamDescriptor =
  StreamDescriptor().name(protocolStreamDescriptor.name).namespace(protocolStreamDescriptor.namespace)

private fun streamDescriptorToClient(
  protocolStreamDescriptor: io.airbyte.protocol.models.v0.StreamDescriptor,
): io.airbyte.api.client.model.generated.StreamDescriptor =
  io.airbyte.api.client.model.generated.StreamDescriptor(
    protocolStreamDescriptor.name,
    protocolStreamDescriptor.namespace,
  )

private fun streamDescriptorToProtocol(apiStreamDescriptor: StreamDescriptor): ProtocolStreamDescriptor =
  io.airbyte.protocol.models.v0
    .StreamDescriptor()
    .withName(apiStreamDescriptor.name)
    .withNamespace(apiStreamDescriptor.namespace)

private fun clientStreamDescriptorToProtocol(clientStreamDescriptor: ClientStreamDescriptor): ProtocolStreamDescriptor =
  io.airbyte.protocol.models.v0
    .StreamDescriptor()
    .withName(clientStreamDescriptor.name)
    .withNamespace(clientStreamDescriptor.namespace)
