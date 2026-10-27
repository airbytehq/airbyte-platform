/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.shared

import io.airbyte.config.JobConfig

data class JobMetadata(
  val id: Long,
  val configType: JobConfig.ConfigType,
  val scope: String,
)
