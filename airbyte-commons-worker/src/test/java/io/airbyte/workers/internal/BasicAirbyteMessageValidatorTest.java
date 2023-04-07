/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.workers.test_utils.AirbyteMessageUtils;
import org.junit.Test;

@SuppressWarnings("MissingJavadocType")
public class BasicAirbyteMessageValidatorTest {

  @Test
  void testValid() {
    final AirbyteMessage record1 = AirbyteMessageUtils.createRecordMessage("stream_1", "field_1", "green");

    final var m = BasicAirbyteMessageValidator.validate(record1);
    assertTrue(m.isPresent());
    assertEquals(record1, m.get());
  }

}
