/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.workers.test_utils.AirbyteMessageUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SimpleAirbyteSourceTest {

  protected static final String STREAM_NAME = "stream1";
  protected static final String FIELD_NAME = "field1";

  protected static final AirbyteMessage RECORD_MESSAGE1 = AirbyteMessageUtils.createRecordMessage(STREAM_NAME, FIELD_NAME, "m1");
  protected static final AirbyteMessage RECORD_MESSAGE2 = AirbyteMessageUtils.createRecordMessage(STREAM_NAME, FIELD_NAME, "m2");
  protected static final AirbyteMessage STATE_MESSAGE = AirbyteMessageUtils.createStateMessage(STREAM_NAME, "checkpoint", "1");

  private SimpleAirbyteSource source;

  @BeforeEach
  void beforeEach() {
    source = new SimpleAirbyteSource();
  }

  @Test
  void testMessages() {
    source.setMessages(RECORD_MESSAGE1, RECORD_MESSAGE2, STATE_MESSAGE);

    // Reading all the messages from the source
    final List<AirbyteMessage> messagesRead = new ArrayList<>();
    while (!source.isFinished()) {
      messagesRead.add(source.attemptRead().get());
    }

    // Once the source is finished, subsequent attemptRead should return emtpy
    assertEquals(Optional.empty(), source.attemptRead());

    assertEquals(List.of(RECORD_MESSAGE1, RECORD_MESSAGE2, STATE_MESSAGE), messagesRead);
  }

  @Test
  void testInfiniteMessages() {
    source.setInfiniteSourceWithMessages(RECORD_MESSAGE1, RECORD_MESSAGE2, STATE_MESSAGE);

    // Reading 10 the messages from the source
    final List<AirbyteMessage> messagesRead = new ArrayList<>();
    for (int i = 0; i < 10; ++i) {
      assertFalse(source.isFinished());
      messagesRead.add(source.attemptRead().get());
    }

    // source should be looping on the 3 messages set in the init call
    assertEquals(List.of(
        RECORD_MESSAGE1, RECORD_MESSAGE2, STATE_MESSAGE,
        RECORD_MESSAGE1, RECORD_MESSAGE2, STATE_MESSAGE,
        RECORD_MESSAGE1, RECORD_MESSAGE2, STATE_MESSAGE,
        RECORD_MESSAGE1), messagesRead);
    assertFalse(source.isFinished());
  }

}
