/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

/**
 * Result of a connect probe. An unreachable host is a normal result, not a workload failure.
 *
 * [errorCode] is [ConnectErrorCode.NONE] when [reachable] is true. [connectLatencyMs] is the time
 * spent on the probe, whether it succeeded or not.
 */
data class ConnectOutput(
  val reachable: Boolean,
  val errorCode: ConnectErrorCode,
  val message: String,
  val resolvedAddresses: List<String> = emptyList(),
  val connectLatencyMs: Long,
)

enum class ConnectErrorCode {
  NONE,
  DNS_FAILED,
  CONNECTION_REFUSED,
  TIMEOUT,
  UNKNOWN,
}
