/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

data class ConnectionWithLatestJob(
  val connection: ConnectionSummary,
  val job: Job?,
)
