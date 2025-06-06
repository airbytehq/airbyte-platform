/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron.jobs

import io.airbyte.config.secrets.SecretCoordinate
import io.airbyte.config.secrets.persistence.SecretPersistence
import io.airbyte.data.services.SecretConfigService
import io.airbyte.domain.models.SecretConfig
import io.airbyte.domain.models.SecretConfigId
import io.airbyte.domain.models.SecretStorageId
import io.airbyte.domain.services.secrets.SecretPersistenceService
import io.airbyte.featureflag.CleanupDanglingSecretConfigs
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.SecretStorage
import io.airbyte.metrics.MetricClient
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.util.UUID

class OrphanedSecretConfigCleanupTest {
  private val secretConfigService = mockk<SecretConfigService>()
  private val secretPersistenceService = mockk<SecretPersistenceService>()
  private val metricClient = mockk<MetricClient>(relaxed = true)
  private val featureFlagClient = mockk<FeatureFlagClient>()
  private val secretPersistence = mockk<SecretPersistence>()

  private val cleanup =
    OrphanedSecretConfigCleanup(
      secretConfigService,
      secretPersistenceService,
      metricClient,
      featureFlagClient,
    )

  @BeforeEach
  fun reset() {
    clearAllMocks()
    every { secretConfigService.deleteByIds(any()) } just runs
  }

  private fun createSecretConfig(
    id: UUID = UUID.randomUUID(),
    secretStorageId: UUID = UUID.randomUUID(),
    descriptor: String = "test-config",
    externalCoordinate: String = SecretCoordinate.AirbyteManagedSecretCoordinate().fullCoordinate,
  ): SecretConfig =
    SecretConfig(
      id = SecretConfigId(id),
      secretStorageId = secretStorageId,
      descriptor = descriptor,
      externalCoordinate = externalCoordinate,
      tombstone = false,
      airbyteManaged = true,
      createdBy = UUID.randomUUID(),
      updatedBy = UUID.randomUUID(),
      createdAt = OffsetDateTime.now().minusHours(2),
      updatedAt = OffsetDateTime.now().minusHours(2),
    )

  @Test
  fun `cleanup successfully deletes orphaned secret configs when feature flag is enabled`() {
    val storageId = UUID.randomUUID()
    val orphanedConfig1 = createSecretConfig(secretStorageId = storageId)
    val orphanedConfig2 = createSecretConfig(secretStorageId = storageId)
    val orphanedConfigs = listOf(orphanedConfig1, orphanedConfig2)

    // Mock external services
    every { secretConfigService.findAirbyteManagedConfigsWithoutReferences(any(), any()) } returns orphanedConfigs
    every { featureFlagClient.boolVariation(CleanupDanglingSecretConfigs, any<SecretStorage>()) } returns true
    every { secretPersistenceService.getPersistenceByStorageId(SecretStorageId(storageId)) } returns secretPersistence
    every { secretPersistence.delete(any()) } just runs
    every { secretConfigService.deleteByIds(any()) } just runs

    cleanup.cleanupOrphanedSecrets()

    // Verify external secret deletions
    verify(exactly = 2) { secretPersistence.delete(any()) }

    // Verify database cleanup with only successfully deleted configs
    verify { secretConfigService.deleteByIds(listOf(orphanedConfig1.id, orphanedConfig2.id)) }
  }

  @Test
  fun `cleanup skips configs when feature flag is disabled`() {
    val orphanedConfig = createSecretConfig()
    every { secretConfigService.findAirbyteManagedConfigsWithoutReferences(any(), any()) } returns listOf(orphanedConfig)
    every { featureFlagClient.boolVariation(CleanupDanglingSecretConfigs, any<SecretStorage>()) } returns false

    cleanup.cleanupOrphanedSecrets()

    verify(exactly = 0) { secretPersistenceService.getPersistenceByStorageId(any()) }
    verify(exactly = 0) { secretPersistence.delete(any()) }
    verify { secretConfigService.deleteByIds(emptyList()) }
  }

  @Test
  fun `cleanup skips configs with invalid coordinates`() {
    val orphanedConfig = createSecretConfig(externalCoordinate = "invalid-coordinate-format")
    every { secretConfigService.findAirbyteManagedConfigsWithoutReferences(any(), any()) } returns listOf(orphanedConfig)
    every { featureFlagClient.boolVariation(CleanupDanglingSecretConfigs, any<SecretStorage>()) } returns true
    every { secretPersistenceService.getPersistenceByStorageId(SecretStorageId(orphanedConfig.secretStorageId)) } returns secretPersistence

    cleanup.cleanupOrphanedSecrets()

    verify(exactly = 0) { secretPersistence.delete(any()) }
    verify { secretConfigService.deleteByIds(emptyList()) }
  }

  @Test
  fun `cleanup handles external deletion failures gracefully`() {
    val storageId = UUID.randomUUID()
    val orphanedConfig1 = createSecretConfig(secretStorageId = storageId, descriptor = "success")
    val orphanedConfig2 = createSecretConfig(secretStorageId = storageId, descriptor = "failure")
    val orphanedConfigs = listOf(orphanedConfig1, orphanedConfig2)

    every { secretConfigService.findAirbyteManagedConfigsWithoutReferences(any(), any()) } returns orphanedConfigs
    every { featureFlagClient.boolVariation(CleanupDanglingSecretConfigs, any<SecretStorage>()) } returns true
    every { secretPersistenceService.getPersistenceByStorageId(SecretStorageId(storageId)) } returns secretPersistence

    // First deletion succeeds, second fails
    every { secretPersistence.delete(any()) } just runs andThenThrows RuntimeException("External deletion failed")

    cleanup.cleanupOrphanedSecrets()

    // Verify both external deletions were attempted
    verify(exactly = 2) { secretPersistence.delete(any()) }

    // Verify only the successful one is deleted from database
    verify { secretConfigService.deleteByIds(listOf(orphanedConfig1.id)) }
  }

  @Test
  fun `cleanup does nothing when no orphaned configs found`() {
    every { secretConfigService.findAirbyteManagedConfigsWithoutReferences(any(), any()) } returns emptyList()

    cleanup.cleanupOrphanedSecrets()

    verify { secretConfigService.findAirbyteManagedConfigsWithoutReferences(any(), any()) }
    verify(exactly = 0) { featureFlagClient.boolVariation(any(), any<SecretStorage>()) }
    verify(exactly = 0) { secretPersistenceService.getPersistenceByStorageId(any()) }
    verify(exactly = 0) { secretConfigService.deleteByIds(any()) }
    verify { metricClient.count(any(), any(), any()) } // job run metric only
  }

  @Test
  fun `cleanup uses correct time filter for one hour grace period`() {
    val timeSlot = slot<OffsetDateTime>()
    val limitSlot = slot<Int>()
    every { secretConfigService.findAirbyteManagedConfigsWithoutReferences(capture(timeSlot), capture(limitSlot)) } returns emptyList()

    cleanup.cleanupOrphanedSecrets()

    // Verify the date filter is approximately 1 hour ago (within 5 minutes tolerance)
    val capturedTime = timeSlot.captured
    val capturedLimit = limitSlot.captured
    val now = OffsetDateTime.now()
    val oneHourAgo = now.minusHours(1)
    assertTrue(capturedTime.isAfter(oneHourAgo.minusMinutes(5)))
    assertTrue(capturedTime.isBefore(oneHourAgo.plusMinutes(5)))
    assertEquals(10000, capturedLimit)
  }

  @Test
  fun `cleanup handles mixed scenarios with different storage IDs and feature flags`() {
    val storageId1 = UUID.randomUUID()
    val storageId2 = UUID.randomUUID()
    val config1 = createSecretConfig(secretStorageId = storageId1)
    val config2 = createSecretConfig(secretStorageId = storageId2)
    val config3 = createSecretConfig(secretStorageId = storageId1)

    every { secretConfigService.findAirbyteManagedConfigsWithoutReferences(any(), any()) } returns listOf(config1, config2, config3)

    // Feature flag enabled for storage1, disabled for storage2
    every { featureFlagClient.boolVariation(CleanupDanglingSecretConfigs, SecretStorage(storageId1.toString())) } returns true
    every { featureFlagClient.boolVariation(CleanupDanglingSecretConfigs, SecretStorage(storageId2.toString())) } returns false

    val persistence1 = mockk<SecretPersistence>()
    every { secretPersistenceService.getPersistenceByStorageId(SecretStorageId(storageId1)) } returns persistence1
    every { persistence1.delete(any()) } just runs

    cleanup.cleanupOrphanedSecrets()

    // Verify only configs from storage1 are processed
    verify(exactly = 2) { persistence1.delete(any()) } // config1 and config3

    // Verify caching: persistence lookup only called once for storageId1 despite 2 configs
    verify(exactly = 1) { secretPersistenceService.getPersistenceByStorageId(SecretStorageId(storageId1)) }
    verify(exactly = 0) { secretPersistenceService.getPersistenceByStorageId(SecretStorageId(storageId2)) }

    // Verify only storage1 configs are deleted from database
    verify { secretConfigService.deleteByIds(listOf(config1.id, config3.id)) }
  }

  @Test
  fun `cleanup caches persistence objects to avoid redundant lookups`() {
    val storageId = UUID.randomUUID()
    val config1 = createSecretConfig(secretStorageId = storageId)
    val config2 = createSecretConfig(secretStorageId = storageId)
    val config3 = createSecretConfig(secretStorageId = storageId)

    every { secretConfigService.findAirbyteManagedConfigsWithoutReferences(any(), any()) } returns listOf(config1, config2, config3)
    every { featureFlagClient.boolVariation(CleanupDanglingSecretConfigs, any<SecretStorage>()) } returns true
    every { secretPersistenceService.getPersistenceByStorageId(SecretStorageId(storageId)) } returns secretPersistence
    every { secretPersistence.delete(any()) } just runs

    cleanup.cleanupOrphanedSecrets()

    // Verify persistence service is only called once despite having 3 configs with same storage ID
    verify(exactly = 1) { secretPersistenceService.getPersistenceByStorageId(SecretStorageId(storageId)) }

    // Verify all 3 secrets are deleted from external storage
    verify(exactly = 3) { secretPersistence.delete(any()) }

    // Verify all 3 configs are deleted from database
    verify { secretConfigService.deleteByIds(listOf(config1.id, config2.id, config3.id)) }
  }
}
