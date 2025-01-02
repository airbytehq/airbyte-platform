/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.workers.test_utils.AirbyteMessageUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SimpleAirbyteDestinationTest {

  protected static final String STREAM_NAME = "stream1";
  protected static final String FIELD_NAME = "field1";

  protected static final AirbyteMessage RECORD_MESSAGE1 = AirbyteMessageUtils.createRecordMessage(STREAM_NAME, FIELD_NAME, "m1");
  protected static final AirbyteMessage RECORD_MESSAGE2 = AirbyteMessageUtils.createRecordMessage(STREAM_NAME, FIELD_NAME, "m2");
  protected static final AirbyteMessage STATE_MESSAGE1 = AirbyteMessageUtils.createStateMessage(STREAM_NAME, "checkpoint", "1");
  protected static final AirbyteMessage STATE_MESSAGE2 = AirbyteMessageUtils.createStateMessage(STREAM_NAME, "checkpoint", "2");

  private SimpleAirbyteDestination destination;

  @BeforeEach
  void beforeEach() {
    destination = new SimpleAirbyteDestination();
  }

  @Test
  void testNotifyEndOfInputTerminatesTheDestination() throws Exception {
    assertFalse(destination.isFinished());
    destination.notifyEndOfInput();
    assertTrue(destination.isFinished());
  }

  @Test
  void testDestinationEchoesStateMessages() throws Exception {
    destination.accept(RECORD_MESSAGE1);
    destination.accept(RECORD_MESSAGE1);
    destination.accept(STATE_MESSAGE1);
    destination.accept(RECORD_MESSAGE2);
    destination.accept(STATE_MESSAGE2);

    assertEquals(STATE_MESSAGE1, destination.attemptRead().get());
    assertEquals(STATE_MESSAGE2, destination.attemptRead().get());
  }

  @Test
  void testDestinationWillReturnAllStateMessagesBeforeClosing() throws Exception {
    destination.accept(STATE_MESSAGE2);
    destination.accept(STATE_MESSAGE1);
    destination.notifyEndOfInput();

    assertFalse(destination.isFinished());
    assertEquals(STATE_MESSAGE2, destination.attemptRead().get());
    assertEquals(STATE_MESSAGE1, destination.attemptRead().get());
    assertTrue(destination.isFinished());
  }

}
