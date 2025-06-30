/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

/**
 * Max workers by job type.
 */
data class MaxWorkersConfig(
  val maxSpecWorkers: Int,
  @JvmField val maxCheckWorkers: Int,
  val maxDiscoverWorkers: Int,
  @JvmField val maxSyncWorkers: Int,
  val maxNotifyWorkers: Int,
)
