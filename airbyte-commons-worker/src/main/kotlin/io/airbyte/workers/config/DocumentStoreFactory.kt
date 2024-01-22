package io.airbyte.workers.config

import io.airbyte.config.storage.CloudStorageConfigs
import io.airbyte.config.storage.CloudStorageConfigs.WorkerStorageType
import io.airbyte.workers.storage.DocumentStoreClient
import io.airbyte.workers.storage.StateClients
import io.micronaut.context.ApplicationContext
import jakarta.inject.Singleton
import java.nio.file.Path

enum class DocumentType {
  LOGS,
  STATE,
  WORKLOAD_OUTPUTS,
}

/**
 * Factory for the different DocumentStoreClients.
 */
@Singleton
class DocumentStoreFactory(private val configFactory: DocumentStoreConfigFactory) {
  fun get(type: DocumentType): DocumentStoreClient {
    val storageType = configFactory.getStorageType(type)
    val config = configFactory.get(type, storageType)
    return StateClients.create(config, prefix(type))
  }

  /**
   * Defines the prefix to use for a given document store.
   *
   * IMPORTANT: Changing the storage location will orphan already existing kube pods when the new version is deployed!
   */
  private fun prefix(type: DocumentType): Path =
    when (type) {
      DocumentType.STATE -> Path.of("/state")
      DocumentType.WORKLOAD_OUTPUTS -> Path.of("/workload/output")
      else -> throw IllegalArgumentException("Unsupported DocumentType $type")
    }
}

/**
 * Helper to retrieve document store configuration.
 *
 * This is dynamically looking properties to work around the structure of the configuration that contains the type in the
 * middle of the config path. This avoids having to explode explicitly all the configuration through Singletons.
 */
@Singleton
class DocumentStoreConfigFactory(private val applicationContext: ApplicationContext) {
  fun getStorageType(type: DocumentType): WorkerStorageType {
    val storageTypeProperty = "airbyte.cloud.storage.${type.toConfigString()}.type"
    return applicationContext.getProperty(storageTypeProperty, String::class.java)
      .map { WorkerStorageType.valueOf(it.uppercase()) }
      .orElseThrow { throw IllegalArgumentException("Missing property $storageTypeProperty") }
  }

  fun get(
    type: DocumentType,
    storageType: WorkerStorageType,
  ): CloudStorageConfigs {
    val prefix = "airbyte.cloud.storage.${type.toConfigString()}.${storageType.toConfigString()}"
    return when (storageType) {
      WorkerStorageType.GCS ->
        CloudStorageConfigs.gcs(
          CloudStorageConfigs.GcsConfig(
            getProperty("$prefix.bucket"),
            getProperty("$prefix.application-credentials"),
          ),
        )
      WorkerStorageType.LOCAL ->
        CloudStorageConfigs.local(
          CloudStorageConfigs.LocalConfig(
            getProperty("$prefix.root"),
          ),
        )
      WorkerStorageType.MINIO ->
        CloudStorageConfigs.minio(
          CloudStorageConfigs.MinioConfig(
            getProperty("$prefix.bucket"),
            getProperty("$prefix.access-key"),
            getProperty("$prefix.secret-access-key"),
            getProperty("$prefix.endpoint"),
          ),
        )
      WorkerStorageType.S3 ->
        CloudStorageConfigs.s3(
          CloudStorageConfigs.S3Config(
            getProperty("$prefix.bucket"),
            getProperty("$prefix.access-key"),
            getProperty("$prefix.secret-access-key"),
            getProperty("$prefix.region"),
          ),
        )
    }
  }

  private fun getProperty(name: String): String =
    applicationContext.getProperty(name, String::class.java).orElseThrow { throw IllegalArgumentException("Missing property $name") }
}

private fun DocumentType.toConfigString(): String = this.name.lowercase().replace('_', '-')

private fun WorkerStorageType.toConfigString(): String = this.name.lowercase()
