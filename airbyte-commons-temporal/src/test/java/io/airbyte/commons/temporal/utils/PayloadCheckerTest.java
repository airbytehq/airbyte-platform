/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.airbyte.commons.temporal.exception.SizeLimitException;
import org.junit.jupiter.api.Test;

class PayloadCheckerTest {

  record Payload(String data) {}

  @Test
  void testValidPayloadSize() {
    final Payload p = new Payload("1".repeat(PayloadChecker.MAX_PAYLOAD_SIZE_BYTES - "{\"data\":\"\"}".length()));
    assertEquals(p, PayloadChecker.validatePayloadSize(p));
  }

  @Test
  void testInvalidPayloadSize() {
    final Payload p = new Payload("1".repeat(PayloadChecker.MAX_PAYLOAD_SIZE_BYTES));
    assertThrows(SizeLimitException.class, () -> PayloadChecker.validatePayloadSize(p));
  }

}
