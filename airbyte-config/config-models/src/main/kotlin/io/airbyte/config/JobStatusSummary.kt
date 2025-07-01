/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import java.util.UUID

@JvmRecord
data class JobStatusSummary(
  @JvmField val connectionId: UUID,
  @JvmField val createdAt: Long,
  @JvmField val status: JobStatus,
)
