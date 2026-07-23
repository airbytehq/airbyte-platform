/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.model

import java.util.UUID

data class DataplaneConfig(
  val dataplaneId: UUID,
  val dataplaneName: String,
  val dataplaneEnabled: Boolean,
  val dataplaneGroupId: UUID,
  val dataplaneGroupName: String,
  // Nullable for backwards compatibility.
  // This field was added later, so if we run against an older control-plane,
  // the CP will not return org ID.
  val organizationId: UUID?,
)
