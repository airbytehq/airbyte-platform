package io.airbyte.config.helpers

import io.airbyte.config.storage.LocalStorageConfig
import io.airbyte.config.storage.StorageBucketConfig
import io.airbyte.config.storage.StorageConfig
import jakarta.inject.Singleton

/**
 * Describes logging configuration.
 */
@Singleton
class LogConfigs(
  val storageConfig: StorageConfig,
) {
  companion object {
    @JvmField
    val EMPTY: LogConfigs =
      LogConfigs(
        LocalStorageConfig(
          StorageBucketConfig("log", "state", "workload", "payload"),
          "/tmp/local-storage",
        ),
      )
  }
}
