/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models.dataworker

import io.airbyte.domain.models.OrganizationId

/**
 * Every region an organization holds Data Worker capacity in, and the sum across them.
 *
 * [totalAllocatedCapacity] is the total *recorded in the allocation table*, which is not
 * necessarily the capacity the organization is entitled to. An organization whose capacity has not
 * been backfilled out of Stigg yet has no rows here and therefore a total of zero, while still
 * holding a real entitlement.
 */
data class OrganizationDataWorkerAllocations(
  val organizationId: OrganizationId,
  val totalAllocatedCapacity: Double,
  val allocations: List<DataWorkerAllocation>,
)
