/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.bookkeeping.events;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.commons.converters.ConnectorConfigUpdater;
import io.airbyte.protocol.models.AirbyteControlConnectorConfigMessage;
import io.airbyte.protocol.models.AirbyteControlMessage;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import io.airbyte.protocol.models.Config;
import io.airbyte.workers.context.ReplicationContext;
import io.airbyte.workers.internal.bookkeeping.AirbyteMessageOrigin;
import java.io.IOException;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test suite for the {@link AirbyteControlMessageEventListener} class.
 */
class AirbyteControlMessageEventListenerTest {

  private AirbyteControlMessageEventListener messageEventListener;
  private ConnectorConfigUpdater connectorConfigUpdater;

  @BeforeEach
  void setup() {
    connectorConfigUpdater = mock(ConnectorConfigUpdater.class);
    messageEventListener = new AirbyteControlMessageEventListener(connectorConfigUpdater);
  }

  @Test
  void testDestinationControlMessage() throws IOException {
    final AirbyteMessageOrigin airbyteMessageOrigin = AirbyteMessageOrigin.DESTINATION;
    final Config config = mock(Config.class);
    final AirbyteControlConnectorConfigMessage airbyteControlConnectorConfigMessage = mock(AirbyteControlConnectorConfigMessage.class);
    final AirbyteControlMessage airbyteControlMessage = mock(AirbyteControlMessage.class);
    final AirbyteMessage airbyteMessage = mock(AirbyteMessage.class);
    final ReplicationContext ReplicationContext = mock(ReplicationContext.class);
    final UUID destinationId = UUID.randomUUID();

    when(airbyteControlConnectorConfigMessage.getConfig()).thenReturn(config);
    when(airbyteControlMessage.getConnectorConfig()).thenReturn(airbyteControlConnectorConfigMessage);
    when(airbyteControlMessage.getType()).thenReturn(AirbyteControlMessage.Type.CONNECTOR_CONFIG);
    when(airbyteMessage.getType()).thenReturn(Type.CONTROL);
    when(airbyteMessage.getControl()).thenReturn(airbyteControlMessage);
    when(ReplicationContext.getDestinationId()).thenReturn(destinationId);

    final ReplicationAirbyteMessageEvent replicationAirbyteMessageEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, airbyteMessage, ReplicationContext);
    messageEventListener.onApplicationEvent(replicationAirbyteMessageEvent);

    verify(connectorConfigUpdater, times(1)).updateDestination(destinationId, config);
  }

  @Test
  void testSourceControlMessage() throws IOException {
    final AirbyteMessageOrigin airbyteMessageOrigin = AirbyteMessageOrigin.SOURCE;
    final Config config = mock(Config.class);
    final AirbyteControlConnectorConfigMessage airbyteControlConnectorConfigMessage = mock(AirbyteControlConnectorConfigMessage.class);
    final AirbyteControlMessage airbyteControlMessage = mock(AirbyteControlMessage.class);
    final AirbyteMessage airbyteMessage = mock(AirbyteMessage.class);
    final ReplicationContext ReplicationContext = mock(ReplicationContext.class);
    final UUID sourceId = UUID.randomUUID();

    when(airbyteControlConnectorConfigMessage.getConfig()).thenReturn(config);
    when(airbyteControlMessage.getConnectorConfig()).thenReturn(airbyteControlConnectorConfigMessage);
    when(airbyteControlMessage.getType()).thenReturn(AirbyteControlMessage.Type.CONNECTOR_CONFIG);
    when(airbyteMessage.getType()).thenReturn(Type.CONTROL);
    when(airbyteMessage.getControl()).thenReturn(airbyteControlMessage);
    when(ReplicationContext.getSourceId()).thenReturn(sourceId);

    final ReplicationAirbyteMessageEvent replicationAirbyteMessageEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, airbyteMessage, ReplicationContext);
    messageEventListener.onApplicationEvent(replicationAirbyteMessageEvent);

    verify(connectorConfigUpdater, times(1)).updateSource(sourceId, config);
  }

  @Test
  void testInternalControlMessage() throws IOException {
    final AirbyteMessageOrigin airbyteMessageOrigin = AirbyteMessageOrigin.INTERNAL;
    final AirbyteMessage airbyteMessage = mock(AirbyteMessage.class);
    final ReplicationContext ReplicationContext = mock(ReplicationContext.class);

    when(airbyteMessage.getType()).thenReturn(Type.CONTROL);

    final ReplicationAirbyteMessageEvent replicationAirbyteMessageEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, airbyteMessage, ReplicationContext);
    messageEventListener.onApplicationEvent(replicationAirbyteMessageEvent);

    verify(connectorConfigUpdater, times(0)).updateDestination(any(UUID.class), any(Config.class));
    verify(connectorConfigUpdater, times(0)).updateSource(any(UUID.class), any(Config.class));
  }

  @Test
  void testSupportsDestinationEvent() {
    final AirbyteMessageOrigin airbyteMessageOrigin = AirbyteMessageOrigin.DESTINATION;
    final AirbyteMessage airbyteMessage = mock(AirbyteMessage.class);
    final ReplicationContext ReplicationContext = mock(ReplicationContext.class);

    when(airbyteMessage.getType()).thenReturn(Type.CONTROL);

    final ReplicationAirbyteMessageEvent replicationAirbyteMessageEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, airbyteMessage, ReplicationContext);
    assertTrue(messageEventListener.supports(replicationAirbyteMessageEvent));
  }

  @Test
  void testSupportsSourceEvent() {
    final AirbyteMessageOrigin airbyteMessageOrigin = AirbyteMessageOrigin.SOURCE;
    final AirbyteMessage airbyteMessage = mock(AirbyteMessage.class);
    final ReplicationContext ReplicationContext = mock(ReplicationContext.class);

    when(airbyteMessage.getType()).thenReturn(Type.CONTROL);

    final ReplicationAirbyteMessageEvent replicationAirbyteMessageEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, airbyteMessage, ReplicationContext);
    assertTrue(messageEventListener.supports(replicationAirbyteMessageEvent));
  }

  @Test
  void testDoesNotSupportNonControlEvent() {
    final AirbyteMessageOrigin airbyteMessageOrigin = AirbyteMessageOrigin.DESTINATION;
    final AirbyteMessage airbyteMessage = mock(AirbyteMessage.class);
    final ReplicationContext ReplicationContext = mock(ReplicationContext.class);

    when(airbyteMessage.getType()).thenReturn(Type.STATE);

    final ReplicationAirbyteMessageEvent replicationAirbyteMessageEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, airbyteMessage, ReplicationContext);
    assertFalse(messageEventListener.supports(replicationAirbyteMessageEvent));
  }

}
