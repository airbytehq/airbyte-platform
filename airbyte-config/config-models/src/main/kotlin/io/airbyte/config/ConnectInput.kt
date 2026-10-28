/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import java.util.UUID

/**
 * Input for a connect workload: can this data plane open a TCP connection to [host]:[port].
 */
data class ConnectInput(
  val host: String,
  val port: Int,
  val workspaceId: UUID,
  val organizationId: UUID,
  val connectTimeoutMs: Long = 5000,
)
