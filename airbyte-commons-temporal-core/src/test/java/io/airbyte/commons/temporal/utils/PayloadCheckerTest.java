/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import io.airbyte.commons.temporal.exception.SizeLimitException;
import io.airbyte.metrics.lib.MetricClient;
import org.junit.jupiter.api.Test;

class PayloadCheckerTest {

  MetricClient mMetricClient = mock(MetricClient.class);

  PayloadChecker payloadChecker = new PayloadChecker(mMetricClient);

  record Payload(String data) {}

  @Test
  void testValidPayloadSize() {
    final Payload p = new Payload("1".repeat(PayloadChecker.MAX_PAYLOAD_SIZE_BYTES - "{\"data\":\"\"}".length()));
    assertEquals(p, payloadChecker.validatePayloadSize(p));
  }

  @Test
  void testInvalidPayloadSize() {
    final Payload p = new Payload("1".repeat(PayloadChecker.MAX_PAYLOAD_SIZE_BYTES));
    assertThrows(SizeLimitException.class, () -> payloadChecker.validatePayloadSize(p));
  }

}
