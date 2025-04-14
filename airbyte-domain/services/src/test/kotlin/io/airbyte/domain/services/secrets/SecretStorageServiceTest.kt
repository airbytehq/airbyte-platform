/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.secrets

import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.commons.json.Jsons
import io.airbyte.config.Organization
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.SecretConfig
import io.airbyte.domain.models.SecretConfigId
import io.airbyte.domain.models.SecretReference
import io.airbyte.domain.models.SecretReferenceScopeType
import io.airbyte.domain.models.SecretStorage
import io.airbyte.domain.models.SecretStorageId
import io.airbyte.domain.models.SecretStorageScopeType
import io.airbyte.domain.models.SecretStorageWithConfig
import io.airbyte.domain.models.WorkspaceId
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.Optional
import java.util.UUID
import io.airbyte.data.services.OrganizationService as OrganizationRepository
import io.airbyte.data.services.SecretReferenceService as SecretReferenceRepository
import io.airbyte.data.services.SecretStorageService as SecretStorageRepository

class SecretStorageServiceTest {
  private val secretStorageId = SecretStorageId(UUID.randomUUID())
  private val secretConfigId = SecretConfigId(UUID.randomUUID())
  private val workspaceId = WorkspaceId(UUID.randomUUID())
  private val orgId = OrganizationId(UUID.randomUUID())
  private val org =
    mockk<Organization>().also {
      every { it.organizationId } returns orgId.value
    }

  private val secretStorageRepository: SecretStorageRepository = mockk()
  private val organizationRepository: OrganizationRepository = mockk()
  private val secretReferenceRepository: SecretReferenceRepository = mockk()
  private val secretsRepositoryReader: SecretsRepositoryReader = mockk()
  private val secretConfigService: SecretConfigService = mockk()

  private val service =
    SecretStorageService(
      secretStorageRepository,
      organizationRepository,
      secretReferenceRepository,
      secretsRepositoryReader,
      secretConfigService,
    )

  @Nested
  inner class GetById {
    @Test
    fun `should return secret storage when found`() {
      val secretStorage = mockk<SecretStorage>()
      every { secretStorageRepository.findById(secretStorageId) } returns secretStorage

      service.getById(secretStorageId) shouldBe secretStorage
    }

    @Test
    fun `should throw ResourceNotFound problem when ID does not exist`() {
      every { secretStorageRepository.findById(secretStorageId) } returns null

      shouldThrow<ResourceNotFoundProblem> { service.getById(secretStorageId) }
    }
  }

  @Nested
  inner class GetByWorkspaceId {
    @Test
    fun `should return workspace scoped secret storage if available`() {
      val secretStorage = mockk<SecretStorage>()
      every {
        secretStorageRepository.listByScopeTypeAndScopeId(SecretStorageScopeType.WORKSPACE, workspaceId.value)
      } returns listOf(secretStorage)

      service.getByWorkspaceId(workspaceId) shouldBe secretStorage
    }

    @Test
    fun `should return organization scoped secret storage if workspace scoped is not available`() {
      every {
        secretStorageRepository.listByScopeTypeAndScopeId(SecretStorageScopeType.WORKSPACE, workspaceId.value)
      } returns emptyList()
      every { organizationRepository.getOrganizationForWorkspaceId(workspaceId.value) } returns Optional.of(org)
      val orgSecretStorage = mockk<SecretStorage>()
      every {
        secretStorageRepository.listByScopeTypeAndScopeId(SecretStorageScopeType.ORGANIZATION, orgId.value)
      } returns listOf(orgSecretStorage)

      service.getByWorkspaceId(workspaceId) shouldBe orgSecretStorage
    }

    @Test
    fun `should return null if no secret storage found`() {
      every {
        secretStorageRepository.listByScopeTypeAndScopeId(SecretStorageScopeType.WORKSPACE, workspaceId.value)
      } returns emptyList()
      every { organizationRepository.getOrganizationForWorkspaceId(workspaceId.value) } returns Optional.of(org)
      every {
        secretStorageRepository.listByScopeTypeAndScopeId(SecretStorageScopeType.ORGANIZATION, orgId.value)
      } returns emptyList()

      service.getByWorkspaceId(workspaceId) shouldBe null
    }
  }

  @Nested
  inner class HydrateStorageConfig {
    @Test
    fun `should throw UnsupportedOperationException if secret storage is configured from environment`() {
      val secretStorage = mockk<SecretStorage>()
      every { secretStorage.configuredFromEnvironment } returns true

      shouldThrow<UnsupportedOperationException> { service.hydrateStorageConfig(secretStorage) }
    }

    @Test
    fun `should throw ResourceNotFoundProblem if no secret references found`() {
      val secretStorage = mockk<SecretStorage>()
      every { secretStorage.configuredFromEnvironment } returns false
      every { secretStorage.id } returns secretStorageId
      every {
        secretReferenceRepository.listByScopeTypeAndScopeId(SecretReferenceScopeType.SECRET_STORAGE, secretStorageId.value)
      } returns emptyList()

      shouldThrow<ResourceNotFoundProblem> { service.hydrateStorageConfig(secretStorage) }
    }

    @Test
    fun `should throw IllegalStateException if multiple secret references found`() {
      val secretStorage = mockk<SecretStorage>()
      every { secretStorage.configuredFromEnvironment } returns false
      every { secretStorage.id } returns secretStorageId
      val secretRef1 = mockk<SecretReference>()
      val secretRef2 = mockk<SecretReference>()
      every {
        secretReferenceRepository.listByScopeTypeAndScopeId(SecretReferenceScopeType.SECRET_STORAGE, secretStorageId.value)
      } returns listOf(secretRef1, secretRef2)

      shouldThrow<IllegalStateException> { service.hydrateStorageConfig(secretStorage) }
    }

    @Test
    fun `should throw ResourceNotFoundProblem if secret config not found`() {
      val secretStorage = mockk<SecretStorage>()
      every { secretStorage.configuredFromEnvironment } returns false
      every { secretStorage.id } returns secretStorageId
      val secretRef = mockk<SecretReference>()
      every {
        secretReferenceRepository.listByScopeTypeAndScopeId(SecretReferenceScopeType.SECRET_STORAGE, secretStorageId.value)
      } returns listOf(secretRef)
      every { secretRef.secretConfigId } returns secretConfigId
      every { secretConfigService.getById(secretConfigId) } throws ResourceNotFoundProblem()

      shouldThrow<ResourceNotFoundProblem> { service.hydrateStorageConfig(secretStorage) }
    }

    @Test
    fun `should return SecretStorageWithConfig when configuration is successfully hydrated`() {
      val secretStorage = mockk<SecretStorage>()
      every { secretStorage.configuredFromEnvironment } returns false
      every { secretStorage.id } returns secretStorageId

      val secretRef = mockk<SecretReference>()
      every {
        secretReferenceRepository.listByScopeTypeAndScopeId(SecretReferenceScopeType.SECRET_STORAGE, secretStorageId.value)
      } returns listOf(secretRef)
      every { secretRef.secretConfigId } returns secretConfigId

      val secretConfig = mockk<SecretConfig>()
      every { secretConfigService.getById(secretConfigId) } returns secretConfig
      val externalCoordinate = "external-coordinate-1_v1"
      every { secretConfig.externalCoordinate } returns externalCoordinate

      val fetchedConfig = Jsons.jsonNode(mapOf("foo" to "bar"))
      // Here we assume that SecretCoordinate.fromFullCoordinate(externalCoordinate) produces a coordinate
      // that when passed to fetchSecretFromDefaultSecretPersistence returns the fetched config.
      every { secretsRepositoryReader.fetchSecretFromDefaultSecretPersistence(any()) } returns fetchedConfig

      val result = service.hydrateStorageConfig(secretStorage)
      result shouldBe SecretStorageWithConfig(secretStorage, fetchedConfig)
    }
  }
}
