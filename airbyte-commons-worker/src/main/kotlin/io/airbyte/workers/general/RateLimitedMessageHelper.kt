/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general

import io.airbyte.api.client.model.generated.StreamStatusRateLimitedMetadata
import io.airbyte.protocol.models.AirbyteStreamStatusReason
import io.airbyte.protocol.models.AirbyteStreamStatusTraceMessage

class RateLimitedMessageHelper {
  companion object {
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
}
