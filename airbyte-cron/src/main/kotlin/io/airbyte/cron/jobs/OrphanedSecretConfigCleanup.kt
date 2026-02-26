/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron.jobs

import io.airbyte.config.secrets.SecretCoordinate
import io.airbyte.config.secrets.persistence.SecretPersistence
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
import io.opentelemetry.instrumentation.annotations.WithSpan
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

  @WithSpan
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

    // First, find distinct storage IDs that have orphaned configs
    val orphanedStorageIds = secretConfigService.findDistinctOrphanedStorageIds(excludeCreatedBefore = oneHourAgo)
    if (orphanedStorageIds.isEmpty()) {
      log.info { "No orphaned secret configs found" }
      return
    }

    // Filter to only storage IDs where the feature flag is enabled
    val enabledStorageIds =
      orphanedStorageIds.filter { storageId ->
        featureFlagClient.boolVariation(CleanupDanglingSecretConfigs, SecretStorage(storageId.toString()))
      }

    if (enabledStorageIds.isEmpty()) {
      log.info { "Found orphaned configs in ${orphanedStorageIds.size} storage(s) but cleanup is not enabled for any of them" }
      return
    }

    log.info { "Found orphaned configs in ${orphanedStorageIds.size} storage(s), cleanup enabled for ${enabledStorageIds.size}" }

    // Now fetch orphaned configs only for enabled storage IDs
    val orphanedConfigs =
      secretConfigService.findAirbyteManagedConfigsWithoutReferencesByStorageIds(
        excludeCreatedBefore = oneHourAgo,
        limit = 50,
        storageIds = enabledStorageIds,
      )

    if (orphanedConfigs.isEmpty()) {
      log.info { "No orphaned secret configs found for enabled storages" }
      return
    }

    log.info { "Found ${orphanedConfigs.size} orphaned secret configs to cleanup" }

    val deletedIds = mutableListOf<SecretConfigId>()
    val secretPersistenceMap = mutableMapOf<UUID, SecretPersistence>()

    // Delete secrets from secret storage
    for (secretConfig in orphanedConfigs) {
      try {
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

        // DRY RUN: Log what would be deleted without actually deleting
        log.info {
          "[DRY RUN] Would delete: ${coordinate.fullCoordinate} from storage ID ${secretConfig.secretStorageId} (persistence: ${secretPersistence::class.simpleName})"
        }
        deletedIds.add(secretConfig.id)

        // TODO: Re-enable actual deletion after dry-run validation
        // secretPersistence.delete(coordinate)

        metricClient.count(
          metric = OssMetricsRegistry.DELETE_SECRET,
          attributes =
            arrayOf(
              MetricAttribute(MetricTags.SUCCESS, "true"),
              MetricAttribute(MetricTags.SECRET_STORAGE_ID, secretConfig.secretStorageId.toString()),
              MetricAttribute(MetricTags.DRY_RUN, "true"),
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

    // DRY RUN: Log what would be deleted from the database without actually deleting
    log.info { "[DRY RUN] Would clean up ${deletedIds.size} orphaned secret configs (IDs: ${deletedIds.map { it.value }})" }

    // TODO: Re-enable actual deletion after dry-run validation
    // secretConfigService.deleteByIds(deletedIds)
    // log.info { "Cleaned up ${deletedIds.size} orphaned secret configs" }
  }
}
