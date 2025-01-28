/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.protocol.models.AirbyteControlMessage;
import io.airbyte.protocol.models.AirbyteErrorTraceMessage;
import io.airbyte.protocol.models.AirbyteEstimateTraceMessage;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.airbyte.protocol.models.AirbyteStreamState;
import io.airbyte.protocol.models.AirbyteStreamStatusTraceMessage;
import io.airbyte.protocol.models.AirbyteTraceMessage;
import io.airbyte.protocol.models.AirbyteTraceMessage.Type;
import io.airbyte.protocol.models.StreamDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test suite for the {@link AirbyteMessageDataExtractor} class.
 */
class AirbyteMessageDataExtractorTest {

  private static final String NAME = "name";
  private static final String NAMESPACE = "namespace";

  private StreamDescriptor defaultValue;
  private StreamDescriptor streamDescriptor;
  private AirbyteMessageDataExtractor airbyteMessageDataExtractor;

  @BeforeEach
  void setup() {
    defaultValue = new StreamDescriptor().withName("default").withNamespace("default");
    streamDescriptor = new StreamDescriptor().withName(NAME).withNamespace(NAMESPACE);
    airbyteMessageDataExtractor = new AirbyteMessageDataExtractor();
  }

  @Test
  void testExtractStreamDescriptorControlMessage() {
    final AirbyteControlMessage airbyteControlMessage = mock(AirbyteControlMessage.class);
    final AirbyteMessage airbyteMessage = new AirbyteMessage().withControl(airbyteControlMessage).withType(AirbyteMessage.Type.CONTROL);
    final StreamDescriptor extracted = airbyteMessageDataExtractor.extractStreamDescriptor(airbyteMessage, defaultValue);
    assertEquals(defaultValue, extracted);
  }

  @Test
  void testExtractStreamDescriptorRecordMessage() {
    final AirbyteRecordMessage airbyteRecordMessage = mock(AirbyteRecordMessage.class);
    when(airbyteRecordMessage.getNamespace()).thenReturn(streamDescriptor.getNamespace());
    when(airbyteRecordMessage.getStream()).thenReturn(streamDescriptor.getName());

    final AirbyteMessage airbyteMessage = new AirbyteMessage().withRecord(airbyteRecordMessage).withType(AirbyteMessage.Type.RECORD);
    final StreamDescriptor extracted = airbyteMessageDataExtractor.extractStreamDescriptor(airbyteMessage, defaultValue);
    assertEquals(streamDescriptor, extracted);
  }

  @Test
  void testExtractStreamDescriptorStateMessage() {
    final AirbyteStateMessage airbyteStateMessage = mock(AirbyteStateMessage.class);
    final AirbyteStreamState airbyteStreamState = mock(AirbyteStreamState.class);

    when(airbyteStreamState.getStreamDescriptor()).thenReturn(streamDescriptor);
    when(airbyteStateMessage.getStream()).thenReturn(airbyteStreamState);

    final AirbyteMessage airbyteMessage = new AirbyteMessage().withState(airbyteStateMessage).withType(AirbyteMessage.Type.STATE);
    final StreamDescriptor extracted = airbyteMessageDataExtractor.extractStreamDescriptor(airbyteMessage, defaultValue);
    assertEquals(streamDescriptor, extracted);
  }

  @Test
  void testExtractStreamDescriptorStateMessageWithoutStream() {
    final AirbyteStateMessage airbyteStateMessage = mock(AirbyteStateMessage.class);
    final AirbyteStreamState airbyteStreamState = mock(AirbyteStreamState.class);

    when(airbyteStreamState.getStreamDescriptor()).thenReturn(streamDescriptor);

    final AirbyteMessage airbyteMessage = new AirbyteMessage().withState(airbyteStateMessage).withType(AirbyteMessage.Type.STATE);
    final StreamDescriptor extracted = airbyteMessageDataExtractor.extractStreamDescriptor(airbyteMessage, defaultValue);
    assertEquals(defaultValue, extracted);
  }

  @Test
  void testExtractStreamDescriptorErrorTraceMessage() {
    final AirbyteErrorTraceMessage airbyteErrorTraceMessage = mock(AirbyteErrorTraceMessage.class);
    final AirbyteTraceMessage airbyteTraceMessage = mock(AirbyteTraceMessage.class);

    when(airbyteErrorTraceMessage.getStreamDescriptor()).thenReturn(streamDescriptor);
    when(airbyteTraceMessage.getType()).thenReturn(Type.ERROR);
    when(airbyteTraceMessage.getError()).thenReturn(airbyteErrorTraceMessage);

    final AirbyteMessage airbyteMessage = new AirbyteMessage().withTrace(airbyteTraceMessage).withType(AirbyteMessage.Type.TRACE);
    final StreamDescriptor extracted = airbyteMessageDataExtractor.extractStreamDescriptor(airbyteMessage, defaultValue);
    assertEquals(streamDescriptor, extracted);
  }

  @Test
  void testExtractStreamDescriptorEstimateTraceMessage() {
    final AirbyteEstimateTraceMessage airbyteEstimateTraceMessage = mock(AirbyteEstimateTraceMessage.class);
    final AirbyteTraceMessage airbyteTraceMessage = mock(AirbyteTraceMessage.class);

    when(airbyteEstimateTraceMessage.getName()).thenReturn(streamDescriptor.getName());
    when(airbyteEstimateTraceMessage.getNamespace()).thenReturn(streamDescriptor.getNamespace());
    when(airbyteTraceMessage.getType()).thenReturn(Type.ESTIMATE);
    when(airbyteTraceMessage.getEstimate()).thenReturn(airbyteEstimateTraceMessage);

    final AirbyteMessage airbyteMessage = new AirbyteMessage().withTrace(airbyteTraceMessage).withType(AirbyteMessage.Type.TRACE);
    final StreamDescriptor extracted = airbyteMessageDataExtractor.extractStreamDescriptor(airbyteMessage, defaultValue);
    assertEquals(streamDescriptor, extracted);
  }

  @Test
  void testExtractStreamDescriptorStreamStatusTraceMessage() {
    final AirbyteStreamStatusTraceMessage airbyteStreamStatusTraceMessage = mock(AirbyteStreamStatusTraceMessage.class);
    final AirbyteTraceMessage airbyteTraceMessage = mock(AirbyteTraceMessage.class);

    when(airbyteStreamStatusTraceMessage.getStreamDescriptor()).thenReturn(streamDescriptor);
    when(airbyteTraceMessage.getType()).thenReturn(Type.STREAM_STATUS);
    when(airbyteTraceMessage.getStreamStatus()).thenReturn(airbyteStreamStatusTraceMessage);

    final AirbyteMessage airbyteMessage = new AirbyteMessage().withTrace(airbyteTraceMessage).withType(AirbyteMessage.Type.TRACE);
    final StreamDescriptor extracted = airbyteMessageDataExtractor.extractStreamDescriptor(airbyteMessage, defaultValue);
    assertEquals(streamDescriptor, extracted);
  }

}
