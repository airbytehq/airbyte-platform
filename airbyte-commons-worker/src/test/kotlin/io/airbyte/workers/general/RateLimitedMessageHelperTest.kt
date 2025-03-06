/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general

import io.airbyte.protocol.models.AirbyteStreamStatusRateLimitedReason
import io.airbyte.protocol.models.AirbyteStreamStatusReason
import io.airbyte.protocol.models.AirbyteStreamStatusTraceMessage
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class RateLimitedMessageHelperTest {
  @Test
  fun `test isStreamStatusRateLimitedMessage with AirbyteStreamStatusTraceMessage - no reasons`() {
    val msg = mockk<AirbyteStreamStatusTraceMessage>()
    every { msg.reasons } returns null
    assertFalse(RateLimitedMessageHelper.isStreamStatusRateLimitedMessage(msg))
  }

  @Test
  fun `test isStreamStatusRateLimitedMessage with AirbyteStreamStatusTraceMessage - multiple reasons`() {
    val reason1 = mockk<AirbyteStreamStatusReason>()
    val reason2 = mockk<AirbyteStreamStatusReason>()
    val msg = mockk<AirbyteStreamStatusTraceMessage>()
    every { msg.reasons } returns listOf(reason1, reason2)
    assertFalse(RateLimitedMessageHelper.isStreamStatusRateLimitedMessage(msg))
  }

  @Test
  fun `test isStreamStatusRateLimitedMessage with AirbyteStreamStatusTraceMessage - correct reason`() {
    val reason = mockk<AirbyteStreamStatusReason>()
    every { reason.type } returns AirbyteStreamStatusReason.AirbyteStreamStatusReasonType.RATE_LIMITED
    val msg = mockk<AirbyteStreamStatusTraceMessage>()
    every { msg.reasons } returns listOf(reason)
    assertTrue(RateLimitedMessageHelper.isStreamStatusRateLimitedMessage(msg))
  }

  @Test
  fun `test extractQuotaResetValue - no reasons`() {
    val msg = mockk<AirbyteStreamStatusTraceMessage>()
    every { msg.reasons } returns null
    assertNull(RateLimitedMessageHelper.extractQuotaResetValue(msg))
  }

  @Test
  fun `test extractQuotaResetValue - multiple reasons`() {
    val reason1 = mockk<AirbyteStreamStatusReason>()
    val reason2 = mockk<AirbyteStreamStatusReason>()
    val msg = mockk<AirbyteStreamStatusTraceMessage>()
    every { msg.reasons } returns listOf(reason1, reason2)
    assertNull(RateLimitedMessageHelper.extractQuotaResetValue(msg))
  }

  @Test
  fun `test extractQuotaResetValue - correct reason with quota reset`() {
    val rateLimited = mockk<AirbyteStreamStatusRateLimitedReason>()
    every { rateLimited.quotaReset } returns 123L
    val reason = mockk<AirbyteStreamStatusReason>()
    every { reason.type } returns AirbyteStreamStatusReason.AirbyteStreamStatusReasonType.RATE_LIMITED
    every { reason.rateLimited } returns rateLimited
    val msg = mockk<AirbyteStreamStatusTraceMessage>()
    every { msg.reasons } returns listOf(reason)
    assertEquals(123L, RateLimitedMessageHelper.extractQuotaResetValue(msg))
  }

  @Test
  fun `test extractQuotaResetValue - correct reason without quota reset`() {
    val rateLimited = mockk<AirbyteStreamStatusRateLimitedReason>()
    every { rateLimited.quotaReset } returns null
    val reason = mockk<AirbyteStreamStatusReason>()
    every { reason.type } returns AirbyteStreamStatusReason.AirbyteStreamStatusReasonType.RATE_LIMITED
    every { reason.rateLimited } returns rateLimited
    val msg = mockk<AirbyteStreamStatusTraceMessage>()
    every { msg.reasons } returns listOf(reason)
    assertNull(RateLimitedMessageHelper.extractQuotaResetValue(msg))
  }
}
