/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import io.airbyte.config.JobConfig.ConfigType

/**
 * JobConfig proxy that abstracts some complexity from the underlying union-like representation.
 */
class JobConfigProxy(
  private val jobConfig: JobConfig?,
) {
  val configType: ConfigType?
    get() = jobConfig?.configType

  val raw: JobConfig?
    get() = jobConfig

  val configuredCatalog: ConfiguredAirbyteCatalog?
    get() =
      when (jobConfig?.configType) {
        ConfigType.SYNC -> jobConfig.sync.configuredAirbyteCatalog
        ConfigType.REFRESH -> jobConfig.refresh.configuredAirbyteCatalog
        ConfigType.RESET_CONNECTION -> jobConfig.resetConnection.configuredAirbyteCatalog
        ConfigType.CLEAR -> jobConfig.resetConnection.configuredAirbyteCatalog
        else -> null
      }
}
