/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.shared

import io.airbyte.config.ConnectionSummary
import io.airbyte.data.services.shared.LatestJobHealthSummary

data class ConnectionWithLatestJob(
  val connection: ConnectionSummary,
  val job: LatestJobHealthSummary?,
)
