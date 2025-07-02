/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import io.airbyte.config.JobConfig.ConfigType

/**
 * This [JobInfo] class contains pieces of information about the parent job that may be useful. This approach was taken as opposed to using the
 * actual [Job] class here to avoid confusion around the fact that the Job instance would not have its `attempts` field populated.
 */
data class JobInfo(
  @JvmField val id: Long,
  val configType: ConfigType,
  val scope: String,
  val config: JobConfig,
  val status: JobStatus,
)
