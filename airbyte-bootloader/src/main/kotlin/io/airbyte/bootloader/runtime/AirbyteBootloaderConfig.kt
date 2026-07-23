/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.bootloader.runtime

import io.micronaut.context.annotation.ConfigurationProperties

internal const val DEFAULT_MIGRATION_BASELINE_VERSION = "0.29.0.001"

@ConfigurationProperties("airbyte.bootloader")
data class AirbyteBootloaderConfig(
  val autoUpgradeConnectors: Boolean = false,
  val migrationBaselineVersion: String = DEFAULT_MIGRATION_BASELINE_VERSION,
  val runMigrationAtStartup: Boolean = true,
)
