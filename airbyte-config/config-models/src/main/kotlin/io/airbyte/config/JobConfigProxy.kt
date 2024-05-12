package io.airbyte.config

import io.airbyte.protocol.models.ConfiguredAirbyteCatalog

/**
 * JobConfig proxy that abstracts some complexity from the underlying union-like representation.
 */
class JobConfigProxy(private val jobConfig: JobConfig) {
  val configuredCatalog: ConfiguredAirbyteCatalog?
    get() =
      when (jobConfig.configType) {
        JobConfig.ConfigType.SYNC -> jobConfig.sync.configuredAirbyteCatalog
        JobConfig.ConfigType.REFRESH -> jobConfig.refresh.configuredAirbyteCatalog
        JobConfig.ConfigType.RESET_CONNECTION -> jobConfig.resetConnection.configuredAirbyteCatalog
        JobConfig.ConfigType.CLEAR -> jobConfig.resetConnection.configuredAirbyteCatalog
        else -> null
      }
}
