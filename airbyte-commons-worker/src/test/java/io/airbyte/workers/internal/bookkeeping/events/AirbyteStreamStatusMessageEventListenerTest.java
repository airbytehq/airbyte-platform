/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.bookkeeping.events;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import io.airbyte.protocol.models.AirbyteStreamStatusTraceMessage;
import io.airbyte.protocol.models.AirbyteTraceMessage;
import io.airbyte.workers.context.ReplicationContext;
import io.airbyte.workers.general.CachingFeatureFlagClient;
import io.airbyte.workers.internal.bookkeeping.AirbyteMessageOrigin;
import io.airbyte.workers.internal.bookkeeping.OldStreamStatusTracker;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test suite for the {@link AirbyteStreamStatusMessageEventListener} class.
 */
class AirbyteStreamStatusMessageEventListenerTest {

  private AirbyteStreamStatusMessageEventListener messageEventListener;
  private OldStreamStatusTracker streamStatusTracker;
  private CachingFeatureFlagClient ffClient;

  private final ReplicationContext replicationContext = new ReplicationContext(true, UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), 0L,
      1, UUID.randomUUID(), "SOURCE_IMAGE", "DESTINATION_IMAGE", UUID.randomUUID(), UUID.randomUUID());

  @BeforeEach
  void setup() {
    streamStatusTracker = mock(OldStreamStatusTracker.class);
    ffClient = mock(CachingFeatureFlagClient.class);
    messageEventListener = new AirbyteStreamStatusMessageEventListener(streamStatusTracker, ffClient);
  }

  @Test
  void testDestinationStatusMessage() {
    final AirbyteMessageOrigin airbyteMessageOrigin = AirbyteMessageOrigin.DESTINATION;
    final AirbyteStreamStatusTraceMessage airbyteStreamStatusTraceMessage = mock(AirbyteStreamStatusTraceMessage.class);
    final AirbyteTraceMessage airbyteTraceMessage = mock(AirbyteTraceMessage.class);
    final AirbyteMessage airbyteMessage = mock(AirbyteMessage.class);

    when(airbyteTraceMessage.getType()).thenReturn(AirbyteTraceMessage.Type.STREAM_STATUS);
    when(airbyteTraceMessage.getStreamStatus()).thenReturn(airbyteStreamStatusTraceMessage);
    when(airbyteMessage.getType()).thenReturn(Type.TRACE);
    when(airbyteMessage.getTrace()).thenReturn(airbyteTraceMessage);

    final ReplicationAirbyteMessageEvent replicationAirbyteMessageEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, airbyteMessage, replicationContext);
    messageEventListener.onApplicationEvent(replicationAirbyteMessageEvent);

    verify(streamStatusTracker, times(1)).track(replicationAirbyteMessageEvent);
  }

  @Test
  void testSourceStatusMessage() {
    final AirbyteMessageOrigin airbyteMessageOrigin = AirbyteMessageOrigin.SOURCE;
    final AirbyteStreamStatusTraceMessage airbyteStreamStatusTraceMessage = mock(AirbyteStreamStatusTraceMessage.class);
    final AirbyteTraceMessage airbyteTraceMessage = mock(AirbyteTraceMessage.class);
    final AirbyteMessage airbyteMessage = mock(AirbyteMessage.class);

    when(airbyteTraceMessage.getType()).thenReturn(AirbyteTraceMessage.Type.STREAM_STATUS);
    when(airbyteTraceMessage.getStreamStatus()).thenReturn(airbyteStreamStatusTraceMessage);
    when(airbyteMessage.getType()).thenReturn(Type.TRACE);
    when(airbyteMessage.getTrace()).thenReturn(airbyteTraceMessage);

    final ReplicationAirbyteMessageEvent replicationAirbyteMessageEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, airbyteMessage, replicationContext);
    messageEventListener.onApplicationEvent(replicationAirbyteMessageEvent);

    verify(streamStatusTracker, times(1)).track(replicationAirbyteMessageEvent);
  }

  @Test
  void testSupportsStatusEvent() {
    final AirbyteMessageOrigin airbyteMessageOrigin = AirbyteMessageOrigin.SOURCE;
    final AirbyteStreamStatusTraceMessage airbyteStreamStatusTraceMessage = mock(AirbyteStreamStatusTraceMessage.class);
    final AirbyteTraceMessage airbyteTraceMessage = mock(AirbyteTraceMessage.class);
    final AirbyteMessage airbyteMessage = mock(AirbyteMessage.class);

    when(airbyteTraceMessage.getType()).thenReturn(AirbyteTraceMessage.Type.STREAM_STATUS);
    when(airbyteTraceMessage.getStreamStatus()).thenReturn(airbyteStreamStatusTraceMessage);
    when(airbyteMessage.getType()).thenReturn(Type.TRACE);
    when(airbyteMessage.getTrace()).thenReturn(airbyteTraceMessage);

    final ReplicationAirbyteMessageEvent replicationAirbyteMessageEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, airbyteMessage, replicationContext);
    assertTrue(messageEventListener.supports(replicationAirbyteMessageEvent));
  }

  @Test
  void testDoesNotSupportNonStatusTraceEvent() {
    final AirbyteMessageOrigin airbyteMessageOrigin = AirbyteMessageOrigin.DESTINATION;
    final AirbyteMessage airbyteMessage = mock(AirbyteMessage.class);
    final AirbyteTraceMessage airbyteTraceMessage = mock(AirbyteTraceMessage.class);

    when(airbyteTraceMessage.getType()).thenReturn(AirbyteTraceMessage.Type.ERROR);
    when(airbyteMessage.getType()).thenReturn(Type.TRACE);
    when(airbyteMessage.getTrace()).thenReturn(airbyteTraceMessage);

    final ReplicationAirbyteMessageEvent replicationAirbyteMessageEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, airbyteMessage, replicationContext);
    assertFalse(messageEventListener.supports(replicationAirbyteMessageEvent));
  }

  @Test
  void testDoesNotSupportNonTraceEvent() {
    final AirbyteMessageOrigin airbyteMessageOrigin = AirbyteMessageOrigin.DESTINATION;
    final AirbyteMessage airbyteMessage = mock(AirbyteMessage.class);

    when(airbyteMessage.getType()).thenReturn(Type.STATE);

    final ReplicationAirbyteMessageEvent replicationAirbyteMessageEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, airbyteMessage, replicationContext);
    assertFalse(messageEventListener.supports(replicationAirbyteMessageEvent));
  }

}
