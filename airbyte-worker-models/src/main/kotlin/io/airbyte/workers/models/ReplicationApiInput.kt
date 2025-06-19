/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.models

import io.airbyte.config.CatalogDiff
import java.util.UUID

data class ReplicationApiInput(
  val connectionId: UUID,
  val jobId: String,
  val attemptId: Long,
  val appliedCatalogDiff: CatalogDiff?,
)
