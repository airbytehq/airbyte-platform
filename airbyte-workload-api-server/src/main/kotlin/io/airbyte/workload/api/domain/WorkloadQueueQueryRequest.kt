/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api.domain

import io.airbyte.config.WorkloadPriority

data class WorkloadQueueQueryRequest(
  var dataplaneGroup: String? = null,
  var priority: WorkloadPriority? = null,
)
