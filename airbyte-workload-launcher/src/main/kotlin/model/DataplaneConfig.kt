/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.model

import java.util.UUID

data class DataplaneConfig(
  val dataplaneId: UUID,
  val dataplaneName: String,
  val dataplaneEnabled: Boolean,
  val dataplaneGroupId: UUID,
  val dataplaneGroupName: String,
)
