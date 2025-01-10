/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.api.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.model.generated.ConnectionState;
import io.airbyte.api.model.generated.ConnectionStateCreateOrUpdate;
import io.airbyte.api.model.generated.ConnectionStateType;
import io.airbyte.api.model.generated.GlobalState;
import io.airbyte.api.model.generated.JobRead;
import io.airbyte.api.model.generated.StreamState;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.errors.SyncIsRunningException;
import io.airbyte.config.StateType;
import io.airbyte.config.StateWrapper;
import io.airbyte.config.persistence.StatePersistence;
import io.airbyte.protocol.models.AirbyteGlobalState;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.airbyte.protocol.models.AirbyteStateMessage.AirbyteStateType;
import io.airbyte.protocol.models.AirbyteStreamState;
import io.airbyte.protocol.models.StreamDescriptor;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StateHandlerTest {

  public static final UUID CONNECTION_ID = UUID.randomUUID();
  private static final JsonNode JSON_BLOB = Jsons.deserialize("{\"users\": 10}");
  public static final StreamDescriptor STREAM_DESCRIPTOR1 = new StreamDescriptor().withName("coffee");
  public static final StreamDescriptor STREAM_DESCRIPTOR2 = new StreamDescriptor().withName("tea");

  private StateHandler stateHandler;
  private StatePersistence statePersistence;
  private JobHistoryHandler jobHistoryHandler;

  @BeforeEach
  void setup() {
    statePersistence = mock(StatePersistence.class);
    jobHistoryHandler = mock(JobHistoryHandler.class);
    stateHandler = new StateHandler(statePersistence, jobHistoryHandler);
  }

  @Test
  void testGetCurrentStateEmpty() throws IOException {
    when(statePersistence.getCurrentState(CONNECTION_ID)).thenReturn(Optional.empty());

    final ConnectionState expected = new ConnectionState().connectionId(CONNECTION_ID).stateType(ConnectionStateType.NOT_SET).streamState(null);
    final ConnectionState actual = stateHandler.getState(new ConnectionIdRequestBody().connectionId(CONNECTION_ID));
    assertEquals(expected, actual);
  }

  @Test
  void testGetLegacyState() throws IOException {
    when(statePersistence.getCurrentState(CONNECTION_ID)).thenReturn(Optional.of(
        new StateWrapper()
            .withStateType(StateType.LEGACY)
            .withLegacyState(JSON_BLOB)));

    final ConnectionState expected = new ConnectionState()
        .connectionId(CONNECTION_ID)
        .stateType(ConnectionStateType.LEGACY)
        .streamState(null)
        .state(JSON_BLOB);
    final ConnectionState actual = stateHandler.getState(new ConnectionIdRequestBody().connectionId(CONNECTION_ID));
    assertEquals(expected, actual);
  }

  @Test
  void testGetGlobalState() throws IOException {
    when(statePersistence.getCurrentState(CONNECTION_ID)).thenReturn(Optional.of(
        new StateWrapper()
            .withStateType(StateType.GLOBAL)
            .withGlobal(new AirbyteStateMessage().withType(AirbyteStateType.GLOBAL).withGlobal(new AirbyteGlobalState()
                .withSharedState(JSON_BLOB)
                .withStreamStates(List.of(
                    new AirbyteStreamState().withStreamDescriptor(STREAM_DESCRIPTOR1).withStreamState(JSON_BLOB),
                    new AirbyteStreamState().withStreamDescriptor(STREAM_DESCRIPTOR2).withStreamState(JSON_BLOB)))))));

    final ConnectionState expected = new ConnectionState()
        .connectionId(CONNECTION_ID)
        .stateType(ConnectionStateType.GLOBAL)
        .streamState(null)
        .globalState(new GlobalState().sharedState(JSON_BLOB).streamStates(List.of(
            new StreamState().streamDescriptor(toApi(STREAM_DESCRIPTOR1)).streamState(JSON_BLOB),
            new StreamState().streamDescriptor(toApi(STREAM_DESCRIPTOR2)).streamState(JSON_BLOB))));
    final ConnectionState actual = stateHandler.getState(new ConnectionIdRequestBody().connectionId(CONNECTION_ID));
    assertEquals(expected, actual);
  }

  @Test
  void testGetStreamState() throws IOException {
    when(statePersistence.getCurrentState(CONNECTION_ID)).thenReturn(Optional.of(
        new StateWrapper()
            .withStateType(StateType.STREAM)
            .withStateMessages(List.of(
                new AirbyteStateMessage()
                    .withType(AirbyteStateType.STREAM)
                    .withStream(new AirbyteStreamState().withStreamDescriptor(STREAM_DESCRIPTOR1).withStreamState(JSON_BLOB)),
                new AirbyteStateMessage()
                    .withType(AirbyteStateType.STREAM)
                    .withStream(new AirbyteStreamState().withStreamDescriptor(STREAM_DESCRIPTOR2).withStreamState(JSON_BLOB))))));

    final ConnectionState expected = new ConnectionState()
        .connectionId(CONNECTION_ID)
        .stateType(ConnectionStateType.STREAM)
        .streamState(List.of(
            new StreamState().streamDescriptor(toApi(STREAM_DESCRIPTOR1)).streamState(JSON_BLOB),
            new StreamState().streamDescriptor(toApi(STREAM_DESCRIPTOR2)).streamState(JSON_BLOB)));
    final ConnectionState actual = stateHandler.getState(new ConnectionIdRequestBody().connectionId(CONNECTION_ID));
    assertEquals(expected, actual);
  }

  // the api type has an extra type, so the verifying the compatibility of the type conversion is more
  // involved
  @Test
  void testEnumConversion() {
    assertEquals(3, AirbyteStateType.class.getEnumConstants().length);
    assertEquals(4, ConnectionStateType.class.getEnumConstants().length);

    // to AirbyteStateType => ConnectionStateType
    assertEquals(ConnectionStateType.GLOBAL, Enums.convertTo(AirbyteStateType.GLOBAL, ConnectionStateType.class));
    assertEquals(ConnectionStateType.STREAM, Enums.convertTo(AirbyteStateType.STREAM, ConnectionStateType.class));
    assertEquals(ConnectionStateType.LEGACY, Enums.convertTo(AirbyteStateType.LEGACY, ConnectionStateType.class));

    // to ConnectionStateType => AirbyteStateType
    assertEquals(AirbyteStateType.GLOBAL, Enums.convertTo(ConnectionStateType.GLOBAL, AirbyteStateType.class));
    assertEquals(AirbyteStateType.STREAM, Enums.convertTo(ConnectionStateType.STREAM, AirbyteStateType.class));
    assertEquals(AirbyteStateType.LEGACY, Enums.convertTo(ConnectionStateType.LEGACY, AirbyteStateType.class));
  }

  @Test
  void testCreateOrUpdateState() throws IOException {
    final ConnectionStateCreateOrUpdate input = new ConnectionStateCreateOrUpdate().connectionId(CONNECTION_ID)
        .connectionState(new ConnectionState().stateType(ConnectionStateType.LEGACY).state(JSON_BLOB));
    stateHandler.createOrUpdateState(input);
    verify(statePersistence, times(1)).updateOrCreateState(CONNECTION_ID,
        new StateWrapper().withStateType(StateType.LEGACY).withLegacyState(JSON_BLOB).withStateMessages(null));
  }

  @Test
  void testCreateOrUpdateStateSafe() throws IOException {
    final ConnectionStateCreateOrUpdate input = new ConnectionStateCreateOrUpdate().connectionId(CONNECTION_ID)
        .connectionState(new ConnectionState().stateType(ConnectionStateType.LEGACY).state(JSON_BLOB));
    when(jobHistoryHandler.getLatestRunningSyncJob(CONNECTION_ID)).thenReturn(Optional.empty());
    stateHandler.createOrUpdateStateSafe(input);
    verify(statePersistence, times(1)).updateOrCreateState(CONNECTION_ID,
        new StateWrapper().withStateType(StateType.LEGACY).withLegacyState(JSON_BLOB).withStateMessages(null));
  }

  @Test
  void testCreateOrUpdateStateSafeThrowsWhenSyncRunning() throws IOException {
    final ConnectionStateCreateOrUpdate input = new ConnectionStateCreateOrUpdate().connectionId(CONNECTION_ID)
        .connectionState(new ConnectionState().stateType(ConnectionStateType.LEGACY).state(JSON_BLOB));
    when(jobHistoryHandler.getLatestRunningSyncJob(CONNECTION_ID)).thenReturn(Optional.of(new JobRead()));
    assertThrows(SyncIsRunningException.class, () -> stateHandler.createOrUpdateStateSafe(input));
  }

  private static io.airbyte.api.model.generated.StreamDescriptor toApi(final StreamDescriptor protocolStreamDescriptor) {
    return new io.airbyte.api.model.generated.StreamDescriptor().name(protocolStreamDescriptor.getName())
        .namespace(protocolStreamDescriptor.getNamespace());
  }

}
