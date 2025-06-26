/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator

import io.airbyte.protocol.models.v0.AirbyteStreamStatusRateLimitedReason
import io.airbyte.protocol.models.v0.AirbyteStreamStatusReason
import io.airbyte.protocol.models.v0.AirbyteStreamStatusTraceMessage
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class RateLimitedMessageHelperTest {
  private lateinit var rateLimitedMessageHelper: RateLimitedMessageHelper

  @BeforeEach
  fun setup() {
    rateLimitedMessageHelper = RateLimitedMessageHelper()
  }

  @Test
  fun `test isStreamStatusRateLimitedMessage with AirbyteStreamStatusTraceMessage - no reasons`() {
    val msg = mockk<AirbyteStreamStatusTraceMessage>()
    every { msg.reasons } returns null
    assertFalse(rateLimitedMessageHelper.isStreamStatusRateLimitedMessage(msg))
  }

  @Test
  fun `test isStreamStatusRateLimitedMessage with AirbyteStreamStatusTraceMessage - multiple reasons`() {
    val reason1 = mockk<AirbyteStreamStatusReason>()
    val reason2 = mockk<AirbyteStreamStatusReason>()
    val msg = mockk<AirbyteStreamStatusTraceMessage>()
    every { msg.reasons } returns listOf(reason1, reason2)
    assertFalse(rateLimitedMessageHelper.isStreamStatusRateLimitedMessage(msg))
  }

  @Test
  fun `test isStreamStatusRateLimitedMessage with AirbyteStreamStatusTraceMessage - correct reason`() {
    val reason = mockk<AirbyteStreamStatusReason>()
    every { reason.type } returns AirbyteStreamStatusReason.AirbyteStreamStatusReasonType.RATE_LIMITED
    val msg = mockk<AirbyteStreamStatusTraceMessage>()
    every { msg.reasons } returns listOf(reason)
    assertTrue(rateLimitedMessageHelper.isStreamStatusRateLimitedMessage(msg))
  }

  @Test
  fun `test extractQuotaResetValue - no reasons`() {
    val msg = mockk<AirbyteStreamStatusTraceMessage>()
    every { msg.reasons } returns null
    Assertions.assertNull(rateLimitedMessageHelper.extractQuotaResetValue(msg))
  }

  @Test
  fun `test extractQuotaResetValue - multiple reasons`() {
    val reason1 = mockk<AirbyteStreamStatusReason>()
    val reason2 = mockk<AirbyteStreamStatusReason>()
    val msg = mockk<AirbyteStreamStatusTraceMessage>()
    every { msg.reasons } returns listOf(reason1, reason2)
    Assertions.assertNull(rateLimitedMessageHelper.extractQuotaResetValue(msg))
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
    Assertions.assertEquals(123L, rateLimitedMessageHelper.extractQuotaResetValue(msg))
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
    Assertions.assertNull(rateLimitedMessageHelper.extractQuotaResetValue(msg))
  }
}
