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
import io.kotest.matchers.shouldNotBe
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkConstructor
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
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

  @Nested
  inner class GetPersistenceFromConfig {
    @Test
    fun `test getPersistenceMapFromConfig with default persistence`() {
      val config =
        ConfigWithSecretReferences(
          Jsons.emptyObject(),
          mapOf(
            "$.password" to
              SecretReferenceConfig(
                secretCoordinate = SecretCoordinate.AirbyteManagedSecretCoordinate(),
              ),
          ),
        )
      val context =
        SecretHydrationContext(
          organizationId,
          workspaceId,
        )

      val persistenceMap = secretPersistenceService.getPersistenceMapFromConfig(config, context)
      assertEquals(defaultSecretPersistence, persistenceMap[null])
    }

    @Test
    fun `test getPersistenceMapFromConfig with storage IDs`() {
      val secretStorageId = UUID.randomUUID()
      val secretStorageId2 = UUID.randomUUID()

      val config =
        ConfigWithSecretReferences(
          Jsons.emptyObject(),
          mapOf(
            "$.password" to
              SecretReferenceConfig(
                secretStorageId = secretStorageId,
                secretCoordinate = SecretCoordinate.ExternalSecretCoordinate("my-secret-coord"),
              ),
            "$.token" to
              SecretReferenceConfig(
                secretStorageId = secretStorageId2,
                secretCoordinate = SecretCoordinate.AirbyteManagedSecretCoordinate(),
              ),
            "$.anotherOne" to
              SecretReferenceConfig(
                secretStorageId = null,
                secretCoordinate = SecretCoordinate.AirbyteManagedSecretCoordinate(),
              ),
          ),
        )

      val context =
        SecretHydrationContext(
          organizationId,
          workspaceId,
        )

      val secretStorage =
        mockk<SecretStorage> {
          every { id } returns SecretStorageId(secretStorageId)
          every { scopeType } returns SecretStorageScopeType.WORKSPACE
          every { scopeId } returns workspaceId.value
          every { storageType } returns SecretStorageType.AWS_SECRETS_MANAGER
          every { isDefault() } returns false
        }
      every { secretStorageService.getById(SecretStorageId(secretStorageId)) } returns secretStorage

      val secretStorage2 =
        mockk<SecretStorage> {
          every { id } returns SecretStorageId(secretStorageId2)
          every { scopeType } returns SecretStorageScopeType.WORKSPACE
          every { scopeId } returns workspaceId.value
          every { storageType } returns SecretStorageType.GOOGLE_SECRET_MANAGER
          every { isDefault() } returns false
        }
      every { secretStorageService.getById(SecretStorageId(secretStorageId2)) } returns secretStorage2

      val storageConfig = Jsons.jsonNode(mapOf("key" to "value"))
      every { secretStorageService.hydrateStorageConfig(any()) } returns
        mockk<SecretStorageWithConfig> {
          every { this@mockk.config } returns storageConfig
        }

      val persistenceMap = secretPersistenceService.getPersistenceMapFromConfig(config, context)
      assertEquals(defaultSecretPersistence, persistenceMap[null])
      assertNotNull(persistenceMap[secretStorageId])
      assertNotNull(persistenceMap[secretStorageId2])

      // we can't mock the constructor for RuntimeSecretPersistence, so we check these for now
      persistenceMap[secretStorageId] shouldNotBe persistenceMap[secretStorageId2]
      persistenceMap[secretStorageId] shouldNotBe defaultSecretPersistence
      persistenceMap[secretStorageId2] shouldNotBe defaultSecretPersistence

      verify {
        secretStorageService.getById(SecretStorageId(secretStorageId))
        secretStorageService.getById(SecretStorageId(secretStorageId2))
        secretStorageService.hydrateStorageConfig(secretStorage)
        secretStorageService.hydrateStorageConfig(secretStorage2)
      }
    }

    @Test
    fun `test getPersistenceMapFromConfig with legacy RuntimeSecretPersistence`() {
      val config =
        ConfigWithSecretReferences(
          Jsons.emptyObject(),
          mapOf(
            "$.password" to
              SecretReferenceConfig(
                secretCoordinate = SecretCoordinate.AirbyteManagedSecretCoordinate(),
              ),
          ),
        )
      val context = SecretHydrationContext(organizationId, workspaceId)
      every { featureFlagClient.boolVariation(eq(UseRuntimeSecretPersistence), any()) } returns true

      every { secretPersistenceConfigService.get(ScopeType.ORGANIZATION, organizationId.value) } returns mockk()

      val persistenceMap = secretPersistenceService.getPersistenceMapFromConfig(config, context)
      assertNotNull(persistenceMap[null])
      persistenceMap shouldNotBe defaultSecretPersistence

      verify { secretPersistenceConfigService.get(ScopeType.ORGANIZATION, organizationId.value) }
    }
  }

  @Nested
  inner class GetPersistenceFromWorkspaceId {
    @Test
    fun `test getPersistenceFromWorkspaceId returns persistence from secret storage`() {
      val secretStorageIdValue = UUID.randomUUID()
      val secretStorageId = SecretStorageId(secretStorageIdValue)
      val secretStorage: SecretStorage =
        mockk {
          every { id } returns secretStorageId
          every { scopeType } returns SecretStorageScopeType.WORKSPACE
          every { scopeId } returns workspaceId.value
          every { storageType } returns SecretStorageType.AWS_SECRETS_MANAGER
          every { isDefault() } returns false
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
    fun `test getPersistenceFromWorkspaceId returns legacy persistence with config from org when no secret storage found`() {
      every { secretStorageService.getByWorkspaceId(workspaceId) } returns null
      val orgId = UUID.randomUUID()
      val org: Organization =
        mockk {
          every { organizationId } returns orgId
        }
      every { organizationService.getOrganizationForWorkspaceId(workspaceId.value) } returns Optional.of(org)
      every { secretPersistenceConfigService.get(ScopeType.ORGANIZATION, orgId) } returns mockk()

      every { featureFlagClient.boolVariation(UseRuntimeSecretPersistence, io.airbyte.featureflag.Organization(orgId)) } returns true

      val persistence = secretPersistenceService.getPersistenceFromWorkspaceId(workspaceId)

      assertNotNull(persistence)
      verify { secretPersistenceConfigService.get(ScopeType.ORGANIZATION, orgId) }
    }

    @Test
    fun `test getPersistenceFromWorkspaceId returns default persistence when no secret storage found`() {
      every { secretStorageService.getByWorkspaceId(workspaceId) } returns null
      val orgId = UUID.randomUUID()
      val org: Organization =
        mockk {
          every { organizationId } returns orgId
        }
      every { organizationService.getOrganizationForWorkspaceId(workspaceId.value) } returns Optional.of(org)
      every { featureFlagClient.boolVariation(UseRuntimeSecretPersistence, io.airbyte.featureflag.Organization(orgId)) } returns false

      val persistence = secretPersistenceService.getPersistenceFromWorkspaceId(workspaceId)

      assertEquals(defaultSecretPersistence, persistence)
      verify(exactly = 0) { secretPersistenceConfigService.get(ScopeType.ORGANIZATION, orgId) }
    }

    @Test
    fun `test getPersistenceFromWorkspaceId throws exception when legacy and no organization is not found`() {
      every { secretStorageService.getByWorkspaceId(workspaceId) } returns null
      every { organizationService.getOrganizationForWorkspaceId(workspaceId.value) } returns Optional.empty()

      assertThrows<IllegalStateException> {
        secretPersistenceService.getPersistenceFromWorkspaceId(workspaceId)
      }
    }
  }
}
