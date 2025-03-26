/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.secrets

import io.airbyte.commons.json.Jsons
import io.airbyte.config.ScopeType
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence
import io.airbyte.config.secrets.persistence.SecretPersistence
import io.airbyte.data.services.SecretPersistenceConfigService
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.SecretStorage
import io.airbyte.domain.models.SecretStorageId
import io.airbyte.domain.models.SecretStorageScopeType
import io.airbyte.domain.models.SecretStorageType
import io.airbyte.domain.models.SecretStorageWithConfig
import io.airbyte.domain.models.WorkspaceId
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.UseRuntimeSecretPersistence
import io.airbyte.metrics.MetricClient
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkConstructor
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID

class SecretPersistenceServiceTest {
  private val defaultSecretPersistence = mockk<SecretPersistence>()
  private val secretStorageService = mockk<SecretStorageService>()
  private val secretPersistenceConfigService = mockk<SecretPersistenceConfigService>()
  private val metricClient = mockk<MetricClient>()
  private val featureFlagClient = mockk<FeatureFlagClient>()

  private val organizationId = OrganizationId(UUID.randomUUID())
  private val workspaceId = WorkspaceId(UUID.randomUUID())

  private val secretPersistenceService =
    SecretPersistenceService(
      defaultSecretPersistence,
      secretStorageService,
      secretPersistenceConfigService,
      metricClient,
      featureFlagClient,
    )

  @BeforeEach
  fun setup() {
    clearAllMocks()
    mockkConstructor(RuntimeSecretPersistence::class)

    every { featureFlagClient.boolVariation(eq(UseRuntimeSecretPersistence), any()) } returns false
  }

  @Test
  fun `test getPersistenceFromConfig with default persistence`() {
    val config = Jsons.emptyObject()
    val context =
      SecretHydrationContext(
        organizationId,
        workspaceId,
      )

    val persistence = secretPersistenceService.getPersistenceFromConfig(config, context)
    assertEquals(defaultSecretPersistence, persistence)
  }

  @Test
  fun `test getPersistenceFromConfig with explicit storage id`() {
    val secretStorageId = UUID.randomUUID()
    val config =
      Jsons.jsonNode(
        mapOf(
          "password" to
            mapOf(
              "_secret" to "my-secret-coord",
              "_secret_storage_id" to secretStorageId.toString(),
            ),
        ),
      )

    val context =
      SecretHydrationContext(
        organizationId,
        workspaceId,
      )

    val secretStorage = mockk<SecretStorage>()
    every { secretStorageService.getById(SecretStorageId(secretStorageId)) } returns secretStorage

    every { secretStorage.scopeId } returns UUID.randomUUID()
    every { secretStorage.scopeType } returns SecretStorageScopeType.WORKSPACE
    every { secretStorage.storageType } returns SecretStorageType.GOOGLE_SECRET_MANAGER

    val withConfig = mockk<SecretStorageWithConfig>()
    val secretStorageConfig = Jsons.jsonNode(mapOf("key" to "value"))
    every { withConfig.config } returns secretStorageConfig
    every { secretStorageService.hydrateStorageConfig(secretStorage) } returns withConfig

    val persistence = secretPersistenceService.getPersistenceFromConfig(config, context)
    assertNotNull(persistence)

    verify {
      secretStorageService.getById(SecretStorageId(secretStorageId))
      secretStorageService.hydrateStorageConfig(secretStorage)
    }
  }

  @Test
  fun `test getPersistenceFromConfig with multiple storage IDs fails`() {
    val config =
      Jsons.jsonNode(
        mapOf(
          "password" to
            mapOf(
              "_secret" to "my-secret-coord",
              "_secret_storage_id" to UUID.randomUUID().toString(),
            ),
          "token" to
            mapOf(
              "_secret" to "my-secret-coord2",
              "_secret_storage_id" to UUID.randomUUID().toString(),
            ),
        ),
      )

    val context =
      SecretHydrationContext(
        organizationId,
        workspaceId,
      )

    assertThrows<IllegalStateException> { secretPersistenceService.getPersistenceFromConfig(config, context) }
  }

  @Test
  fun `test getPersistenceFromConfig with legacy RuntimeSecretPersistence`() {
    val config = Jsons.emptyObject()
    val context = SecretHydrationContext(organizationId, workspaceId)
    every { featureFlagClient.boolVariation(eq(UseRuntimeSecretPersistence), any()) } returns true

    every { secretPersistenceConfigService.get(ScopeType.ORGANIZATION, organizationId.value) } returns mockk()

    val persistence = secretPersistenceService.getPersistenceFromConfig(config, context)
    assertNotNull(persistence)

    verify { secretPersistenceConfigService.get(ScopeType.ORGANIZATION, organizationId.value) }
  }
}
