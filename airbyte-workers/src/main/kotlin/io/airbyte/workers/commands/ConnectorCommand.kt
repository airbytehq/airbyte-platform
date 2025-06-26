/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.commands

import io.airbyte.api.client.model.generated.FailureOrigin
import io.airbyte.api.client.model.generated.FailureType
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.FailureReason
import jakarta.inject.Singleton

interface ConnectorCommand<Input> {
  val name: String

  fun start(
    input: Input,
    signalPayload: String?,
  ): String

  fun isTerminal(id: String): Boolean

  fun getOutput(id: String): ConnectorJobOutput

  fun cancel(id: String)
}

@Singleton
class FailureConverter {
  fun getFailureType(failureType: FailureType?): FailureReason.FailureType? {
    if (failureType == null) {
      return FailureReason.FailureType.SYSTEM_ERROR
    }

    return when (failureType) {
      FailureType.CONFIG_ERROR -> FailureReason.FailureType.CONFIG_ERROR
      FailureType.SYSTEM_ERROR -> FailureReason.FailureType.SYSTEM_ERROR
      FailureType.MANUAL_CANCELLATION -> FailureReason.FailureType.MANUAL_CANCELLATION
      FailureType.REFRESH_SCHEMA -> FailureReason.FailureType.REFRESH_SCHEMA
      FailureType.HEARTBEAT_TIMEOUT -> FailureReason.FailureType.HEARTBEAT_TIMEOUT
      FailureType.DESTINATION_TIMEOUT -> FailureReason.FailureType.DESTINATION_TIMEOUT
      FailureType.TRANSIENT_ERROR -> FailureReason.FailureType.TRANSIENT_ERROR
    }
  }

  fun getFailureOrigin(failureOrigin: FailureOrigin?): FailureReason.FailureOrigin {
    if (failureOrigin == null) {
      return FailureReason.FailureOrigin.UNKNOWN
    }

    return when (failureOrigin) {
      FailureOrigin.SOURCE -> FailureReason.FailureOrigin.SOURCE
      FailureOrigin.DESTINATION -> FailureReason.FailureOrigin.DESTINATION
      FailureOrigin.REPLICATION -> FailureReason.FailureOrigin.REPLICATION
      FailureOrigin.PERSISTENCE -> FailureReason.FailureOrigin.PERSISTENCE
      FailureOrigin.AIRBYTE_PLATFORM -> FailureReason.FailureOrigin.AIRBYTE_PLATFORM
      FailureOrigin.NORMALIZATION -> FailureReason.FailureOrigin.NORMALIZATION
      FailureOrigin.DBT -> FailureReason.FailureOrigin.DBT
      FailureOrigin.UNKNOWN -> FailureReason.FailureOrigin.UNKNOWN
    }
  }
}
