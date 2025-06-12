/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron.jobs

import datadog.trace.api.Trace
import io.airbyte.config.secrets.SecretCoordinate
import io.airbyte.config.secrets.persistence.SecretPersistence
import io.airbyte.cron.SCHEDULED_TRACE_OPERATION_NAME
import io.airbyte.data.services.SecretConfigService
import io.airbyte.domain.models.SecretConfigId
import io.airbyte.domain.models.SecretStorageId
import io.airbyte.domain.services.secrets.SecretPersistenceService
import io.airbyte.featureflag.CleanupDanglingSecretConfigs
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.SecretStorage
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.scheduling.annotation.Scheduled
import jakarta.inject.Singleton
import java.time.OffsetDateTime
import java.util.UUID

private val log = KotlinLogging.logger {}

@Singleton
class OrphanedSecretConfigCleanup(
  private val secretConfigService: SecretConfigService,
  private val secretPersistenceService: SecretPersistenceService,
  private val metricClient: MetricClient,
  private val featureFlagClient: FeatureFlagClient,
) {
  init {
    log.info { "Creating orphaned secret config cleanup job" }
  }

  @Trace(operationName = SCHEDULED_TRACE_OPERATION_NAME)
  @Scheduled(fixedRate = "30m", initialDelay = "5m")
  fun cleanupOrphanedSecrets() {
    log.info { "Starting orphaned secret config cleanup" }

    metricClient.count(
      metric = OssMetricsRegistry.CRON_JOB_RUN_BY_CRON_TYPE,
      attributes = arrayOf(MetricAttribute(MetricTags.CRON_TYPE, "orphaned_secret_cleanup")),
    )

    // Find orphaned configs that were created more than 1 hour ago
    // This gives a grace period for recently created configs that might not have references yet
    val oneHourAgo = OffsetDateTime.now().minusHours(1)
    val orphanedConfigs = secretConfigService.findAirbyteManagedConfigsWithoutReferences(oneHourAgo, 10000)

    if (orphanedConfigs.isEmpty()) {
      log.info { "No orphaned secret configs found" }
      return
    }

    log.info { "Found ${orphanedConfigs.size} orphaned secret configs to cleanup" }

    val deletedIds = mutableListOf<SecretConfigId>()
    val secretPersistenceMap = mutableMapOf<UUID, SecretPersistence>()

    // Delete secrets from secret storage
    for (secretConfig in orphanedConfigs) {
      try {
        if (!featureFlagClient.boolVariation(CleanupDanglingSecretConfigs, SecretStorage(secretConfig.secretStorageId.toString()))) {
          continue
        }

        val secretPersistence =
          secretPersistenceMap.getOrPut(secretConfig.secretStorageId) {
            log.info { "Fetching persistence for storage ID: ${secretConfig.secretStorageId}" }
            val persistence = secretPersistenceService.getPersistenceByStorageId(SecretStorageId(secretConfig.secretStorageId))
            log.info {
              "Got persistence for storage ID ${secretConfig.secretStorageId};  Persistence type: ${persistence::class.simpleName}"
            }
            persistence
          }

        val coordinate = SecretCoordinate.AirbyteManagedSecretCoordinate.fromFullCoordinate(secretConfig.externalCoordinate)
        if (coordinate == null) {
          log.warn { "Skipping deletion for invalid coordinate ${secretConfig.externalCoordinate} in storage ${secretConfig.secretStorageId}" }
          continue
        }

        log.info {
          "Deleting: ${coordinate.fullCoordinate} from storage ID ${secretConfig.secretStorageId} (persistence: ${secretPersistence::class.simpleName})"
        }
        secretPersistence.delete(coordinate)
        deletedIds.add(secretConfig.id)

        metricClient.count(
          metric = OssMetricsRegistry.DELETE_SECRET,
          attributes =
            arrayOf(
              MetricAttribute(MetricTags.SUCCESS, "true"),
              MetricAttribute(MetricTags.SECRET_STORAGE_ID, secretConfig.secretStorageId.toString()),
            ),
        )
      } catch (e: Exception) {
        log.error(e) { "Failed to delete secret ${secretConfig.externalCoordinate} in storage ${secretConfig.secretStorageId}" }
        metricClient.count(
          metric = OssMetricsRegistry.DELETE_SECRET,
          attributes =
            arrayOf(
              MetricAttribute(MetricTags.SUCCESS, "false"),
              MetricAttribute(MetricTags.SECRET_STORAGE_ID, secretConfig.secretStorageId.toString()),
            ),
        )
      }
    }

    // Delete the orphaned secret configs from the database
    secretConfigService.deleteByIds(deletedIds)
    log.info { "Cleaned up ${deletedIds.size} orphaned secret configs" }
  }
}
