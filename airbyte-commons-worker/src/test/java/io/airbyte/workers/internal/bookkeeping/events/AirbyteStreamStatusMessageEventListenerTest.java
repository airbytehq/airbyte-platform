/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
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
import io.airbyte.workers.internal.bookkeeping.AirbyteMessageOrigin;
import io.airbyte.workers.internal.bookkeeping.StreamStatusTracker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test suite for the {@link AirbyteStreamStatusMessageEventListener} class.
 */
class AirbyteStreamStatusMessageEventListenerTest {

  private AirbyteStreamStatusMessageEventListener messageEventListener;
  private StreamStatusTracker streamStatusTracker;

  @BeforeEach
  void setup() {
    streamStatusTracker = mock(StreamStatusTracker.class);
    messageEventListener = new AirbyteStreamStatusMessageEventListener(streamStatusTracker);
  }

  @Test
  void testDestinationStatusMessage() {
    final AirbyteMessageOrigin airbyteMessageOrigin = AirbyteMessageOrigin.DESTINATION;
    final AirbyteStreamStatusTraceMessage airbyteStreamStatusTraceMessage = mock(AirbyteStreamStatusTraceMessage.class);
    final AirbyteTraceMessage airbyteTraceMessage = mock(AirbyteTraceMessage.class);
    final AirbyteMessage airbyteMessage = mock(AirbyteMessage.class);
    final ReplicationContext ReplicationContext = mock(ReplicationContext.class);

    when(airbyteTraceMessage.getType()).thenReturn(AirbyteTraceMessage.Type.STREAM_STATUS);
    when(airbyteTraceMessage.getStreamStatus()).thenReturn(airbyteStreamStatusTraceMessage);
    when(airbyteMessage.getType()).thenReturn(Type.TRACE);
    when(airbyteMessage.getTrace()).thenReturn(airbyteTraceMessage);

    final ReplicationAirbyteMessageEvent replicationAirbyteMessageEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, airbyteMessage, ReplicationContext);
    messageEventListener.onApplicationEvent(replicationAirbyteMessageEvent);

    verify(streamStatusTracker, times(1)).track(replicationAirbyteMessageEvent);
  }

  @Test
  void testSourceStatusMessage() {
    final AirbyteMessageOrigin airbyteMessageOrigin = AirbyteMessageOrigin.SOURCE;
    final AirbyteStreamStatusTraceMessage airbyteStreamStatusTraceMessage = mock(AirbyteStreamStatusTraceMessage.class);
    final AirbyteTraceMessage airbyteTraceMessage = mock(AirbyteTraceMessage.class);
    final AirbyteMessage airbyteMessage = mock(AirbyteMessage.class);
    final ReplicationContext ReplicationContext = mock(ReplicationContext.class);

    when(airbyteTraceMessage.getType()).thenReturn(AirbyteTraceMessage.Type.STREAM_STATUS);
    when(airbyteTraceMessage.getStreamStatus()).thenReturn(airbyteStreamStatusTraceMessage);
    when(airbyteMessage.getType()).thenReturn(Type.TRACE);
    when(airbyteMessage.getTrace()).thenReturn(airbyteTraceMessage);

    final ReplicationAirbyteMessageEvent replicationAirbyteMessageEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, airbyteMessage, ReplicationContext);
    messageEventListener.onApplicationEvent(replicationAirbyteMessageEvent);

    verify(streamStatusTracker, times(1)).track(replicationAirbyteMessageEvent);
  }

  @Test
  void testSupportsStatusEvent() {
    final AirbyteMessageOrigin airbyteMessageOrigin = AirbyteMessageOrigin.SOURCE;
    final AirbyteStreamStatusTraceMessage airbyteStreamStatusTraceMessage = mock(AirbyteStreamStatusTraceMessage.class);
    final AirbyteTraceMessage airbyteTraceMessage = mock(AirbyteTraceMessage.class);
    final AirbyteMessage airbyteMessage = mock(AirbyteMessage.class);
    final ReplicationContext ReplicationContext = mock(ReplicationContext.class);

    when(airbyteTraceMessage.getType()).thenReturn(AirbyteTraceMessage.Type.STREAM_STATUS);
    when(airbyteTraceMessage.getStreamStatus()).thenReturn(airbyteStreamStatusTraceMessage);
    when(airbyteMessage.getType()).thenReturn(Type.TRACE);
    when(airbyteMessage.getTrace()).thenReturn(airbyteTraceMessage);

    final ReplicationAirbyteMessageEvent replicationAirbyteMessageEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, airbyteMessage, ReplicationContext);
    assertTrue(messageEventListener.supports(replicationAirbyteMessageEvent));
  }

  @Test
  void testDoesNotSupportNonStatusTraceEvent() {
    final AirbyteMessageOrigin airbyteMessageOrigin = AirbyteMessageOrigin.DESTINATION;
    final AirbyteMessage airbyteMessage = mock(AirbyteMessage.class);
    final AirbyteTraceMessage airbyteTraceMessage = mock(AirbyteTraceMessage.class);
    final ReplicationContext ReplicationContext = mock(ReplicationContext.class);

    when(airbyteTraceMessage.getType()).thenReturn(AirbyteTraceMessage.Type.ERROR);
    when(airbyteMessage.getType()).thenReturn(Type.TRACE);
    when(airbyteMessage.getTrace()).thenReturn(airbyteTraceMessage);

    final ReplicationAirbyteMessageEvent replicationAirbyteMessageEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, airbyteMessage, ReplicationContext);
    assertFalse(messageEventListener.supports(replicationAirbyteMessageEvent));
  }

  @Test
  void testDoesNotSupportNonTraceEvent() {
    final AirbyteMessageOrigin airbyteMessageOrigin = AirbyteMessageOrigin.DESTINATION;
    final AirbyteMessage airbyteMessage = mock(AirbyteMessage.class);
    final ReplicationContext ReplicationContext = mock(ReplicationContext.class);

    when(airbyteMessage.getType()).thenReturn(Type.STATE);

    final ReplicationAirbyteMessageEvent replicationAirbyteMessageEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, airbyteMessage, ReplicationContext);
    assertFalse(messageEventListener.supports(replicationAirbyteMessageEvent));
  }

}
