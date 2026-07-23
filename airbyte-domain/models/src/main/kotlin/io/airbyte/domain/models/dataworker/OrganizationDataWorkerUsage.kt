/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models.dataworker

import io.airbyte.domain.models.OrganizationId

data class OrganizationDataWorkerUsage(
  val organizationId: OrganizationId,
  val dataplaneGroups: List<DataplaneGroupDataWorkerUsage>,
)
