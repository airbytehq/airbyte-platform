/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.secrets

import io.airbyte.commons.json.Jsons
import io.airbyte.config.Organization
import io.airbyte.config.ScopeType
import io.airbyte.config.secrets.ConfigWithSecretReferences
import io.airbyte.config.secrets.SecretCoordinate
import io.airbyte.config.secrets.SecretReferenceConfig
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence
import io.airbyte.config.secrets.persistence.SecretPersistence
import io.airbyte.data.services.OrganizationService
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
import java.util.Optional
import java.util.UUID

class SecretPersistenceServiceTest {
  private val defaultSecretPersistence = mockk<SecretPersistence>()
  private val secretStorageService = mockk<SecretStorageService>()
  private val secretPersistenceConfigService = mockk<SecretPersistenceConfigService>()
  private val metricClient = mockk<MetricClient>()
  private val featureFlagClient = mockk<FeatureFlagClient>()
  private val organizationService = mockk<OrganizationService>()

  private val organizationId = OrganizationId(UUID.randomUUID())
  private val workspaceId = WorkspaceId(UUID.randomUUID())

  private val secretPersistenceService =
    SecretPersistenceService(
      defaultSecretPersistence,
      secretStorageService,
      secretPersistenceConfigService,
      metricClient,
      featureFlagClient,
      organizationService,
    )

  @BeforeEach
  fun setup() {
    clearAllMocks()
    mockkConstructor(RuntimeSecretPersistence::class)

    every { featureFlagClient.boolVariation(eq(UseRuntimeSecretPersistence), any()) } returns false
  }

  @Test
  fun `test getPersistenceFromConfig with default persistence`() {
    val config = ConfigWithSecretReferences(Jsons.emptyObject(), mapOf())
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
      ConfigWithSecretReferences(
        Jsons.emptyObject(),
        mapOf(
          "$.password" to
            SecretReferenceConfig(
              secretStorageId = secretStorageId,
              secretCoordinate = SecretCoordinate.ExternalSecretCoordinate("_my_coord"),
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
      ConfigWithSecretReferences(
        Jsons.emptyObject(),
        mapOf(
          "$.password" to
            SecretReferenceConfig(
              secretStorageId = UUID.randomUUID(),
              secretCoordinate = SecretCoordinate.ExternalSecretCoordinate("my-secret-coord"),
            ),
          "$.token" to
            SecretReferenceConfig(
              secretStorageId = UUID.randomUUID(),
              secretCoordinate = SecretCoordinate.AirbyteManagedSecretCoordinate(),
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
    val config = ConfigWithSecretReferences(Jsons.emptyObject(), mapOf())
    val context = SecretHydrationContext(organizationId, workspaceId)
    every { featureFlagClient.boolVariation(eq(UseRuntimeSecretPersistence), any()) } returns true

    every { secretPersistenceConfigService.get(ScopeType.ORGANIZATION, organizationId.value) } returns mockk()

    val persistence = secretPersistenceService.getPersistenceFromConfig(config, context)
    assertNotNull(persistence)

    verify { secretPersistenceConfigService.get(ScopeType.ORGANIZATION, organizationId.value) }
  }

  @Test
  fun `test getPersistenceFromWorkspaceId returns persistence from secret storage if available`() {
    val secretStorageIdValue = UUID.randomUUID()
    val secretStorageId = SecretStorageId(secretStorageIdValue)
    val secretStorage: SecretStorage =
      mockk {
        every { id } returns secretStorageId
        every { scopeType } returns SecretStorageScopeType.WORKSPACE
        every { scopeId } returns workspaceId.value
        every { storageType } returns SecretStorageType.AWS_SECRETS_MANAGER
      }
    every { secretStorageService.getByWorkspaceId(workspaceId) } returns secretStorage
    every { secretStorageService.getById(secretStorageId) } returns secretStorage

    val secretStorageConfig = Jsons.jsonNode(mapOf("key" to "value"))
    val secretStorageWithConfig = mockk<SecretStorageWithConfig>()
    every { secretStorageWithConfig.config } returns secretStorageConfig
    every { secretStorageService.hydrateStorageConfig(secretStorage) } returns secretStorageWithConfig

    val persistence = secretPersistenceService.getPersistenceFromWorkspaceId(workspaceId)

    assertNotNull(persistence)
    verify { secretStorageService.getByWorkspaceId(workspaceId) }
    verify { secretStorageService.getById(secretStorageId) }
    verify { secretStorageService.hydrateStorageConfig(secretStorage) }
  }

  @Test
  fun `test getPersistenceFromWorkspaceId returns legacy persistence when no secret storage and feature flag is true`() {
    every { secretStorageService.getByWorkspaceId(workspaceId) } returns null
    val orgId = UUID.randomUUID()
    val org: Organization =
      mockk {
        every { organizationId } returns orgId
      }

    every { organizationService.getOrganizationForWorkspaceId(workspaceId.value) } returns Optional.of(org)
    every { featureFlagClient.boolVariation(eq(UseRuntimeSecretPersistence), any()) } returns true

    every { secretPersistenceConfigService.get(ScopeType.ORGANIZATION, orgId) } returns mockk()

    val persistence = secretPersistenceService.getPersistenceFromWorkspaceId(workspaceId)

    assertNotNull(persistence)
    verify { secretPersistenceConfigService.get(ScopeType.ORGANIZATION, orgId) }
  }

  @Test
  fun `test getPersistenceFromWorkspaceId returns default persistence when no secret storage and feature flag is false`() {
    every { secretStorageService.getByWorkspaceId(workspaceId) } returns null
    every { organizationService.getOrganizationForWorkspaceId(workspaceId.value) } returns
      Optional.of(
        mockk {
          every { organizationId } returns UUID.randomUUID()
        },
      )
    every { featureFlagClient.boolVariation(eq(UseRuntimeSecretPersistence), any()) } returns false

    val persistence = secretPersistenceService.getPersistenceFromWorkspaceId(workspaceId)

    assertEquals(defaultSecretPersistence, persistence)
  }

  @Test
  fun `test getPersistenceFromWorkspaceId throws exception when organization is not found`() {
    every { secretStorageService.getByWorkspaceId(workspaceId) } returns null
    every { organizationService.getOrganizationForWorkspaceId(workspaceId.value) } returns Optional.empty()

    assertThrows<IllegalStateException> {
      secretPersistenceService.getPersistenceFromWorkspaceId(workspaceId)
    }
  }
}
