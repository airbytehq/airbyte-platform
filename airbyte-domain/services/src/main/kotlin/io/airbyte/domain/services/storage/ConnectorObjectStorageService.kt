/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.storage

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.domain.models.ConnectionId
import io.airbyte.domain.models.RejectedRecordsMetadata
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton

val log = KotlinLogging.logger {}

const val OBJECT_STORAGE_TYPE_CONFIG_KEY = "storage_type"
const val S3_STORAGE_TYPE = "S3"

@Singleton
class ConnectorObjectStorageService(
  private val actorDefinitionService: ActorDefinitionService,
  private val connectionService: ConnectionService,
  private val destinationService: DestinationService,
) {
  @JvmName("getRejectedRecordsForJob")
  fun getRejectedRecordsForJob(
    connectionId: ConnectionId,
    job: Job,
    rejectedRecordCount: Long,
  ): RejectedRecordsMetadata? {
    if (rejectedRecordCount <= 0) {
      return null
    }

    val destinationVersion = getJobDestinationVersion(job)
    if (destinationVersion == null) {
      log.warn { "Job ${job.id} does not have a valid destination version, cannot retrieve rejected records metadata." }
      return null
    }

    try {
      val objectStorageConfigProperty = getObjectStorageConfigProperty(destinationVersion)
      return if (objectStorageConfigProperty != null) {
        val connection = connectionService.getStandardSync(connectionId.value)
        val destination = destinationService.getDestinationConnection(connection.destinationId)
        getRejectedRecordsFromDestination(destination.configuration, objectStorageConfigProperty, job.id)
      } else {
        null
      }
    } catch (e: Exception) {
      // Never fail due to errors generating rejected records metadata
      log.error(e) { "Failed to get rejected records metadata for job ${job.id} in connection $connectionId" }
      return null
    }
  }

  private fun getRejectedRecordsFromDestination(
    destinationConfig: JsonNode,
    objectStorageConfigPath: String,
    jobId: Long,
  ): RejectedRecordsMetadata? {
    val objectStorageConfig = destinationConfig.get(objectStorageConfigPath)

    if (objectStorageConfig == null || objectStorageConfig.isNull) {
      return null
    }

    val storageType = objectStorageConfig.get(OBJECT_STORAGE_TYPE_CONFIG_KEY)?.asText()
    if (storageType == null) {
      return null
    }

    // Instantiate the appropriate bucket link resolver based on storage type
    val metaResolver: ObjectStoragePathResolver =
      when (storageType) {
        S3_STORAGE_TYPE -> S3ObjectStoragePathResolver(objectStorageConfig)
        else -> return null // Unsupported storage type
      }

    return metaResolver.resolveRejectedRecordsPaths(jobId)
  }

  private fun getJobDestinationVersion(job: Job): ActorDefinitionVersion? {
    if (job.config?.configType != JobConfig.ConfigType.SYNC) return null

    val destinationVersionId = job.config.sync.destinationDefinitionVersionId
    return try {
      actorDefinitionService.getActorDefinitionVersion(destinationVersionId)
    } catch (e: ConfigNotFoundException) {
      null
    }
  }

  fun getObjectStorageConfigProperty(destinationVersion: ActorDefinitionVersion): String? {
    // In the future, we may want to grab the specific field from the spec, once we do bucket injection
    // For now, we assume all data activation destination will have the bucket config at `objectStorageConfig`
    if (destinationVersion.supportsDataActivation) {
      return "object_storage_config"
    }

    return null
  }
}
