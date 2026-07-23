/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api.domain

data class WorkloadQueueCleanLimit(
  var limit: Int = 1000,
)
