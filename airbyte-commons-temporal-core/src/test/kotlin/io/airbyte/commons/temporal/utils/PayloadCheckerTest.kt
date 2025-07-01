/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.utils

import io.airbyte.commons.temporal.exception.SizeLimitException
import io.airbyte.metrics.MetricClient
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.mockito.Mockito

internal class PayloadCheckerTest {
  var mMetricClient: MetricClient = Mockito.mock(MetricClient::class.java)

  var payloadChecker: PayloadChecker = PayloadChecker(mMetricClient)

  @JvmRecord
  internal data class Payload(
    val data: String,
  )

  @Test
  fun testValidPayloadSize() {
    val p = Payload("1".repeat(PayloadChecker.MAX_PAYLOAD_SIZE_BYTES - "{\"data\":\"\"}".length))
    Assertions.assertEquals(p, payloadChecker.validatePayloadSize(p))
  }

  @Test
  fun testInvalidPayloadSize() {
    val p = Payload("1".repeat(PayloadChecker.MAX_PAYLOAD_SIZE_BYTES))
    Assertions.assertThrows(
      SizeLimitException::class.java,
    ) { payloadChecker.validatePayloadSize(p) }
  }
}
