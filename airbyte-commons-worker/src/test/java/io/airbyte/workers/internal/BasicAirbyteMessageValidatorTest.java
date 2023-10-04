/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.airbyte.commons.json.Jsons;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.Config;
import io.airbyte.workers.test_utils.AirbyteMessageUtils;
import java.util.Optional;
import org.junit.Test;

public class BasicAirbyteMessageValidatorTest {

  @Test
  void testObviousInvalid() {
    final Optional<AirbyteMessage> bad = Jsons.tryDeserialize("{}", AirbyteMessage.class);

    final var m = BasicAirbyteMessageValidator.validate(bad.get());
    assertTrue(m.isEmpty());
  }

  @Test
  void testValidRecord() {
    final AirbyteMessage rec = AirbyteMessageUtils.createRecordMessage("stream_1", "field_1", "green");

    final var m = BasicAirbyteMessageValidator.validate(rec);
    assertTrue(m.isPresent());
    assertEquals(rec, m.get());
  }

  @Test
  void testSubtleInvalidRecord() {
    final Optional<AirbyteMessage> bad = Jsons.tryDeserialize("{\"type\": \"RECORD\", \"record\": {}}", AirbyteMessage.class);

    final var m = BasicAirbyteMessageValidator.validate(bad.get());
    assertTrue(m.isEmpty());
  }

  @Test
  void testValidState() {
    final AirbyteMessage rec = AirbyteMessageUtils.createStateMessage(1);

    final var m = BasicAirbyteMessageValidator.validate(rec);
    assertTrue(m.isPresent());
    assertEquals(rec, m.get());
  }

  @Test
  void testSubtleInvalidState() {
    final Optional<AirbyteMessage> bad = Jsons.tryDeserialize("{\"type\": \"STATE\", \"control\": {}}", AirbyteMessage.class);

    final var m = BasicAirbyteMessageValidator.validate(bad.get());
    assertTrue(m.isEmpty());
  }

  @Test
  void testValidControl() {
    final AirbyteMessage rec = AirbyteMessageUtils.createConfigControlMessage(new Config(), 1000.0);

    final var m = BasicAirbyteMessageValidator.validate(rec);
    assertTrue(m.isPresent());
    assertEquals(rec, m.get());
  }

  @Test
  void testSubtleInvalidControl() {
    final Optional<AirbyteMessage> bad = Jsons.tryDeserialize("{\"type\": \"CONTROL\", \"state\": {}}", AirbyteMessage.class);

    final var m = BasicAirbyteMessageValidator.validate(bad.get());
    assertTrue(m.isEmpty());
  }

}
