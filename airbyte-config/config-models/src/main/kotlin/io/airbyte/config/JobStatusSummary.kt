/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import java.util.UUID

data class JobStatusSummary(
  @JvmField val connectionId: UUID,
  @JvmField val createdAt: Long,
  @JvmField val status: JobStatus,
)
