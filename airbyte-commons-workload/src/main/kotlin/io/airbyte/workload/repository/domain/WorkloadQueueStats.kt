/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.repository.domain

import io.micronaut.core.annotation.Introspected
import io.micronaut.core.annotation.Nullable

@Introspected
data class WorkloadQueueStats(
  @Nullable
  var dataplaneGroup: String? = null,
  @Nullable
  var priority: Int? = null,
  var enqueuedCount: Long,
)
