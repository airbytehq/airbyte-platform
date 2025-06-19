/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator

import io.airbyte.api.client.model.generated.StreamStatusRateLimitedMetadata
import io.airbyte.protocol.models.v0.AirbyteStreamStatusReason
import io.airbyte.protocol.models.v0.AirbyteStreamStatusTraceMessage
import jakarta.inject.Singleton

@Singleton
class RateLimitedMessageHelper {
  fun apiFromProtocol(msg: AirbyteStreamStatusTraceMessage): StreamStatusRateLimitedMetadata? {
    val quotaResetValue = extractQuotaResetValue(msg) ?: return null

    return StreamStatusRateLimitedMetadata(quotaReset = quotaResetValue)
  }

  fun isStreamStatusRateLimitedMessage(msg: AirbyteStreamStatusTraceMessage): Boolean =
    when {
      msg.reasons?.size != 1 -> false
      msg.reasons[0]?.type != AirbyteStreamStatusReason.AirbyteStreamStatusReasonType.RATE_LIMITED -> false
      else -> true
    }

  fun extractQuotaResetValue(msg: AirbyteStreamStatusTraceMessage): Long? =
    when {
      msg.reasons?.size != 1 -> null
      msg.reasons[0]?.type != AirbyteStreamStatusReason.AirbyteStreamStatusReasonType.RATE_LIMITED -> null
      else -> msg.reasons[0]?.rateLimited?.quotaReset
    }
}
