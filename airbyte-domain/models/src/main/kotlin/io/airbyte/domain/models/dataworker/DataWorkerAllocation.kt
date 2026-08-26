/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models.dataworker

import io.airbyte.domain.models.DataplaneGroupId

/**
 * Data Worker capacity an organization holds in a single dataplane group (region).
 */
data class DataWorkerAllocation(
  val dataplaneGroupId: DataplaneGroupId,
  val allocatedCapacity: Double,
)
