package io.airbyte.config.helpers

import io.airbyte.config.storage.DefaultGcsClientFactory
import io.airbyte.config.storage.DefaultS3ClientFactory
import io.airbyte.config.storage.GcsStorageConfig
import io.airbyte.config.storage.MinioS3ClientFactory
import io.airbyte.config.storage.MinioStorageConfig
import io.airbyte.config.storage.S3StorageConfig
import java.io.File
import java.io.IOException

/**
 * Interface for various Cloud Storage clients supporting Cloud log retrieval.
 *
 * The underlying assumption 1) each file at the path is part of the entire log file represented by
 * that path 2) log files names start with timestamps, making it possible extract the time the file
 * was written from its name.
 */
interface CloudLogs {
  /**
   * Retrieve all objects at the given path in lexicographical order, and return their contents as one
   * file.
   */
  @Throws(IOException::class)
  fun downloadCloudLog(
    configs: LogConfigs,
    logPath: String,
  ): File

  /**
   * Assume all the lexicographically ordered objects at the given path form one giant log file,
   * return the last numLines lines.
   */
  @Throws(IOException::class)
  fun tailCloudLog(
    configs: LogConfigs,
    logPath: String,
    numLines: Int,
  ): List<String>

  fun deleteLogs(
    configs: LogConfigs,
    logPath: String,
  )

  companion object {
    /**
     * Create cloud log client.
     *
     * @param configs log configs
     * @return log client
     */
    @JvmStatic
    fun createCloudLogClient(configs: LogConfigs): CloudLogs =
      when (val storageConfig = configs.storageConfig) {
        is GcsStorageConfig -> GcsLogs(DefaultGcsClientFactory(storageConfig))
        is MinioStorageConfig -> S3Logs(MinioS3ClientFactory(storageConfig))
        is S3StorageConfig -> S3Logs(DefaultS3ClientFactory(storageConfig))
        else -> throw IllegalArgumentException("type ${storageConfig::class} is unsupported")
      }
  }
}
