/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.secrets

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.commons.json.Jsons
import io.airbyte.config.Organization
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.data.repositories.DataplaneGroupRepository
import io.airbyte.data.repositories.DataplaneRepository
import io.airbyte.data.repositories.WorkspaceRepository
import io.airbyte.data.repositories.entities.Dataplane
import io.airbyte.data.repositories.entities.DataplaneGroup
import io.airbyte.data.repositories.entities.Workspace
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.PatchField.Companion.toPatch
import io.airbyte.domain.models.SecretConfig
import io.airbyte.domain.models.SecretConfigId
import io.airbyte.domain.models.SecretReference
import io.airbyte.domain.models.SecretReferenceScopeType
import io.airbyte.domain.models.SecretStorage
import io.airbyte.domain.models.SecretStorageCreate
import io.airbyte.domain.models.SecretStorageId
import io.airbyte.domain.models.SecretStorageScopeType
import io.airbyte.domain.models.SecretStorageType
import io.airbyte.domain.models.SecretStorageWithConfig
import io.airbyte.domain.models.UserId
import io.airbyte.domain.models.WorkspaceId
import io.airbyte.featureflag.EnableDefaultSecretStorage
import io.airbyte.featureflag.TestClient
import io.airbyte.featureflag.UseRuntimeSecretPersistence
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.Optional
import java.util.UUID
import io.airbyte.data.services.OrganizationService as OrganizationRepository
import io.airbyte.data.services.SecretReferenceService as SecretReferenceRepository
import io.airbyte.data.services.SecretStorageService as SecretStorageRepository

class SecretStorageServiceTest {
  private val userId = UserId(UUID.randomUUID())
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
  private val featureFlagClient: TestClient = mockk()
  private val workspaceRepository: WorkspaceRepository = mockk()
  private val dataplaneRepository: DataplaneRepository = mockk()
  private val dataplaneGroupRepository: DataplaneGroupRepository = mockk()

  private val service =
    SecretStorageService(
      secretStorageRepository,
      organizationRepository,
      secretReferenceRepository,
      secretsRepositoryReader,
      secretConfigService,
      featureFlagClient,
      workspaceRepository,
      dataplaneRepository,
      dataplaneGroupRepository,
    )

  @Nested
  inner class GetById {
    @Test
    fun `should return secret storage when found`() {
      val secretStorage = mockk<SecretStorage>()
      every { secretStorageRepository.findById(secretStorageId) } returns secretStorage

      assertEquals(secretStorage, service.getById(secretStorageId))
    }

    @Test
    fun `should throw ResourceNotFound problem when ID does not exist`() {
      every { secretStorageRepository.findById(secretStorageId) } returns null

      assertThrows<ResourceNotFoundProblem> { service.getById(secretStorageId) }
    }
  }

  @Nested
  inner class Create {
    @Test
    fun `should create secret storage`() {
      val secretStorageCreate =
        SecretStorageCreate(
          scopeType = SecretStorageScopeType.ORGANIZATION,
          scopeId = orgId.value,
          descriptor = "descriptor",
          storageType = SecretStorageType.LOCAL_TESTING,
          configuredFromEnvironment = false,
          createdBy = userId,
        )
      val storageConfig = Jsons.jsonNode(mapOf("key" to "value"))

      val secretStorage =
        mockk<SecretStorage> {
          every { id } returns secretStorageId
        }
      every { secretStorageRepository.create(secretStorageCreate) } returns secretStorage

      assertEquals(secretStorage, service.createSecretStorage(secretStorageCreate, storageConfig))

      verify {
        secretStorageRepository.create(secretStorageCreate)
      }
    }

    @Test
    fun `should create env-configured secret storage`() {
      val secretStorageCreate =
        SecretStorageCreate(
          scopeType = SecretStorageScopeType.ORGANIZATION,
          scopeId = orgId.value,
          descriptor = "descriptor",
          storageType = SecretStorageType.AWS_SECRETS_MANAGER,
          configuredFromEnvironment = true,
          createdBy = userId,
        )

      val secretStorage =
        mockk<SecretStorage> {
          every { id } returns secretStorageId
        }
      every { secretStorageRepository.create(secretStorageCreate) } returns secretStorage

      assertEquals(secretStorage, service.createSecretStorage(secretStorageCreate, null))

      verify {
        secretStorageRepository.create(secretStorageCreate)
      }
    }

    @Test
    fun `should throw when storage config is null for non-env-configured storage`() {
      val secretStorageCreate =
        SecretStorageCreate(
          scopeType = SecretStorageScopeType.ORGANIZATION,
          scopeId = orgId.value,
          descriptor = "descriptor",
          storageType = SecretStorageType.AWS_SECRETS_MANAGER,
          configuredFromEnvironment = false,
          createdBy = userId,
        )

      assertThrows<IllegalArgumentException> {
        service.createSecretStorage(secretStorageCreate, null)
      }
    }

    private fun secretStorageCreate(storageType: SecretStorageType) =
      SecretStorageCreate(
        scopeType = SecretStorageScopeType.WORKSPACE,
        scopeId = workspaceId.value,
        descriptor = "descriptor",
        storageType = storageType,
        configuredFromEnvironment = false,
        createdBy = userId,
      )

    private val validAzureConfig =
      mapOf(
        "vaultUrl" to "https://example.vault.azure.net",
        "tenantId" to "tenant",
        "clientId" to "client",
        "clientSecret" to "super-secret-value",
      )

    @Test
    fun `should create secret storage with valid azure key vault config`() {
      val create = secretStorageCreate(SecretStorageType.AZURE_KEY_VAULT)
      val secretStorage = mockk<SecretStorage>()
      every { secretStorageRepository.create(create) } returns secretStorage

      assertEquals(secretStorage, service.createSecretStorage(create, Jsons.jsonNode(validAzureConfig + ("extraKey" to "allowed"))))
    }

    @Test
    fun `should reject config with nested object values`() {
      val create = secretStorageCreate(SecretStorageType.AZURE_KEY_VAULT)
      val wrappedConfig =
        Jsons.jsonNode(
          mapOf(
            "scopeType" to "workspace",
            "scopeId" to workspaceId.value.toString(),
            "secretStorageType" to "azure_key_vault",
            "configuration" to validAzureConfig,
          ),
        )

      val problem =
        assertThrows<BadRequestProblem> {
          service.createSecretStorage(create, wrappedConfig)
        }
      assertTrue(
        problem.problem
          .getData()
          .toString()
          .contains("configuration"),
        problem.problem.getData().toString(),
      )
      assertFalse(
        problem.problem
          .getData()
          .toString()
          .contains("super-secret-value"),
        problem.problem.getData().toString(),
      )
    }

    @Test
    fun `should reject config that is not a JSON object`() {
      val create = secretStorageCreate(SecretStorageType.AZURE_KEY_VAULT)

      assertThrows<BadRequestProblem> {
        service.createSecretStorage(create, Jsons.jsonNode("just a string"))
      }
    }

    @Test
    fun `should reject config with non-string scalar values`() {
      val create = secretStorageCreate(SecretStorageType.AZURE_KEY_VAULT)
      val config = Jsons.jsonNode(validAzureConfig + ("someFlag" to true))

      val problem =
        assertThrows<BadRequestProblem> {
          service.createSecretStorage(create, config)
        }
      assertTrue(
        problem.problem
          .getData()
          .toString()
          .contains("someFlag"),
        problem.problem.getData().toString(),
      )
    }

    @Test
    fun `should reject azure config with missing required keys`() {
      val create = secretStorageCreate(SecretStorageType.AZURE_KEY_VAULT)
      val config = Jsons.jsonNode(validAzureConfig - "tenantId" - "clientSecret")

      val problem =
        assertThrows<BadRequestProblem> {
          service.createSecretStorage(create, config)
        }
      assertTrue(
        problem.problem
          .getData()
          .toString()
          .contains("clientSecret"),
        problem.problem.getData().toString(),
      )
      assertTrue(
        problem.problem
          .getData()
          .toString()
          .contains("tenantId"),
        problem.problem.getData().toString(),
      )
    }

    @Test
    fun `should create secret storage with valid vault config`() {
      val create = secretStorageCreate(SecretStorageType.VAULT)
      val secretStorage = mockk<SecretStorage>()
      every { secretStorageRepository.create(create) } returns secretStorage

      val config = Jsons.jsonNode(mapOf("address" to "http://vault:8200", "token" to "vault-token", "prefix" to "abc"))

      assertEquals(secretStorage, service.createSecretStorage(create, config))
    }

    @Test
    fun `should create vault storage with blank prefix`() {
      val create = secretStorageCreate(SecretStorageType.VAULT)
      val secretStorage = mockk<SecretStorage>()
      every { secretStorageRepository.create(create) } returns secretStorage

      val config = Jsons.jsonNode(mapOf("address" to "http://vault:8200", "token" to "vault-token", "prefix" to ""))

      assertEquals(secretStorage, service.createSecretStorage(create, config))
    }

    @Test
    fun `should reject vault config without prefix key`() {
      val create = secretStorageCreate(SecretStorageType.VAULT)
      val config = Jsons.jsonNode(mapOf("address" to "http://vault:8200", "token" to "vault-token"))

      val problem =
        assertThrows<BadRequestProblem> {
          service.createSecretStorage(create, config)
        }
      assertTrue(
        problem.problem
          .getData()
          .toString()
          .contains("prefix"),
        problem.problem.getData().toString(),
      )
    }

    @Test
    fun `should reject config with blank required values`() {
      val create = secretStorageCreate(SecretStorageType.VAULT)
      val config = Jsons.jsonNode(mapOf("address" to "http://vault:8200", "token" to "", "prefix" to "abc"))

      val problem =
        assertThrows<BadRequestProblem> {
          service.createSecretStorage(create, config)
        }
      assertTrue(
        problem.problem
          .getData()
          .toString()
          .contains("token"),
        problem.problem.getData().toString(),
      )
    }

    @Test
    fun `should create aws secret storage with valid access key config and no auth_type`() {
      val create = secretStorageCreate(SecretStorageType.AWS_SECRETS_MANAGER)
      val secretStorage = mockk<SecretStorage>()
      every { secretStorageRepository.create(create) } returns secretStorage

      val config =
        Jsons.jsonNode(
          mapOf(
            "awsRegion" to "us-east-1",
            "awsAccessKey" to "AKIA123",
            "awsSecretAccessKey" to "aws-secret-value",
          ),
        )

      assertEquals(secretStorage, service.createSecretStorage(create, config))
    }

    @Test
    fun `should reject aws access key config with missing required keys`() {
      val create = secretStorageCreate(SecretStorageType.AWS_SECRETS_MANAGER)
      val config =
        Jsons.jsonNode(
          mapOf(
            "auth_type" to "ACCESS_KEY",
            "awsRegion" to "us-east-1",
          ),
        )

      val problem =
        assertThrows<BadRequestProblem> {
          service.createSecretStorage(create, config)
        }
      assertTrue(
        problem.problem
          .getData()
          .toString()
          .contains("awsAccessKey"),
        problem.problem.getData().toString(),
      )
      assertTrue(
        problem.problem
          .getData()
          .toString()
          .contains("awsSecretAccessKey"),
        problem.problem.getData().toString(),
      )
    }

    @Test
    fun `should create aws secret storage with valid iam role config without optional keys`() {
      val create = secretStorageCreate(SecretStorageType.AWS_SECRETS_MANAGER)
      val secretStorage = mockk<SecretStorage>()
      every { secretStorageRepository.create(create) } returns secretStorage

      val config =
        Jsons.jsonNode(
          mapOf(
            "auth_type" to "iam_role",
            "awsRegion" to "us-east-1",
            "roleArn" to "arn:aws:iam::123456789012:role/airbyte",
            "externalId" to "external-id",
          ),
        )

      assertEquals(secretStorage, service.createSecretStorage(create, config))
    }

    @Test
    fun `should reject aws iam role config with missing required keys`() {
      val create = secretStorageCreate(SecretStorageType.AWS_SECRETS_MANAGER)
      val config =
        Jsons.jsonNode(
          mapOf(
            "auth_type" to "IAM_ROLE",
            "awsRegion" to "us-east-1",
            "kmsKeyArn" to "arn:aws:kms:us-east-1:123456789012:key/abc",
          ),
        )

      val problem =
        assertThrows<BadRequestProblem> {
          service.createSecretStorage(create, config)
        }
      assertTrue(
        problem.problem
          .getData()
          .toString()
          .contains("externalId"),
        problem.problem.getData().toString(),
      )
      assertTrue(
        problem.problem
          .getData()
          .toString()
          .contains("roleArn"),
        problem.problem.getData().toString(),
      )
    }

    @Test
    fun `should reject aws config with unknown auth_type`() {
      val create = secretStorageCreate(SecretStorageType.AWS_SECRETS_MANAGER)
      val config =
        Jsons.jsonNode(
          mapOf(
            "auth_type" to "ACESS_KEY",
            "awsRegion" to "us-east-1",
          ),
        )

      val problem =
        assertThrows<BadRequestProblem> {
          service.createSecretStorage(create, config)
        }
      assertTrue(
        problem.problem
          .getData()
          .toString()
          .contains("ACESS_KEY"),
        problem.problem.getData().toString(),
      )
      assertTrue(
        problem.problem
          .getData()
          .toString()
          .contains("ACCESS_KEY"),
        problem.problem.getData().toString(),
      )
      assertTrue(
        problem.problem
          .getData()
          .toString()
          .contains("IAM_ROLE"),
        problem.problem.getData().toString(),
      )
    }

    @Test
    fun `should create secret storage with valid google secret manager config`() {
      val create = secretStorageCreate(SecretStorageType.GOOGLE_SECRET_MANAGER)
      val secretStorage = mockk<SecretStorage>()
      every { secretStorageRepository.create(create) } returns secretStorage

      val config =
        Jsons.jsonNode(
          mapOf(
            "gcpProjectId" to "my-project",
            "gcpCredentialsJson" to """{"type": "service_account"}""",
          ),
        )

      assertEquals(secretStorage, service.createSecretStorage(create, config))
    }

    @Test
    fun `should reject google secret manager config with missing required keys`() {
      val create = secretStorageCreate(SecretStorageType.GOOGLE_SECRET_MANAGER)
      val config = Jsons.jsonNode(mapOf("gcpProjectId" to "my-project"))

      val problem =
        assertThrows<BadRequestProblem> {
          service.createSecretStorage(create, config)
        }
      assertTrue(
        problem.problem
          .getData()
          .toString()
          .contains("gcpCredentialsJson"),
        problem.problem.getData().toString(),
      )
    }
  }

  @Nested
  inner class Disable {
    @Test
    fun `should disable secret storage`() {
      val secretStorage = mockk<SecretStorage>()
      every { secretStorageRepository.patch(secretStorageId, tombstone = true.toPatch(), updatedBy = userId) } returns secretStorage

      assertEquals(secretStorage, service.deleteSecretStorage(secretStorageId, userId))

      verify { secretStorageRepository.patch(secretStorageId, tombstone = true.toPatch(), updatedBy = userId) }
    }
  }

  @Nested
  inner class GetByWorkspaceId {
    @BeforeEach
    fun setup() {
      every { organizationRepository.getOrganizationForWorkspaceId(workspaceId.value) } returns Optional.of(org)
      every { featureFlagClient.boolVariation(UseRuntimeSecretPersistence, any()) } returns false
    }

    @Test
    fun `should return workspace scoped secret storage if available`() {
      val secretStorage =
        mockk<SecretStorage> {
          every { tombstone } returns false
        }
      every {
        secretStorageRepository.listByScopeTypeAndScopeId(SecretStorageScopeType.WORKSPACE, workspaceId.value)
      } returns listOf(secretStorage)

      assertEquals(secretStorage, service.getByWorkspaceId(workspaceId))
    }

    @Test
    fun `should return organization scoped secret storage if workspace scoped is not available`() {
      every { featureFlagClient.boolVariation(any(), any()) } returns true
      every {
        secretStorageRepository.listByScopeTypeAndScopeId(SecretStorageScopeType.WORKSPACE, workspaceId.value)
      } returns emptyList()
      every { organizationRepository.getOrganizationForWorkspaceId(workspaceId.value) } returns Optional.of(org)
      val orgSecretStorage =
        mockk<SecretStorage> {
          every { tombstone } returns false
        }
      every {
        secretStorageRepository.listByScopeTypeAndScopeId(SecretStorageScopeType.ORGANIZATION, orgId.value)
      } returns listOf(orgSecretStorage)

      assertEquals(orgSecretStorage, service.getByWorkspaceId(workspaceId))
    }

    @Test
    fun `should return default if no workspace or organization-scoped secret storage found`() {
      val expected = mockk<SecretStorage>()
      every {
        secretStorageRepository.listByScopeTypeAndScopeId(SecretStorageScopeType.WORKSPACE, workspaceId.value)
      } returns emptyList()
      every { organizationRepository.getOrganizationForWorkspaceId(workspaceId.value) } returns Optional.of(org)
      every {
        secretStorageRepository.listByScopeTypeAndScopeId(SecretStorageScopeType.ORGANIZATION, orgId.value)
      } returns emptyList()
      every {
        secretStorageRepository.findById(SecretStorage.DEFAULT_SECRET_STORAGE_ID)
      } returns expected

      every {
        featureFlagClient.boolVariation(eq(EnableDefaultSecretStorage), any())
      } returns true

      assertEquals(expected, service.getByWorkspaceId(workspaceId))
    }
  }

  @Nested
  inner class ListForDataplane {
    private val serviceAccountId = UUID.randomUUID()
    private val dataplaneGroupId = UUID.randomUUID()
    private val orgStorage = storage(SecretStorageScopeType.ORGANIZATION, orgId.value)
    private val defaultStorage =
      storage(
        SecretStorageScopeType.ORGANIZATION,
        DEFAULT_ORGANIZATION_ID,
      ).copy(id = SecretStorage.DEFAULT_SECRET_STORAGE_ID)

    @BeforeEach
    fun setup() {
      every { dataplaneRepository.findByServiceAccountId(serviceAccountId) } returns
        Dataplane(dataplaneGroupId = dataplaneGroupId, name = "dataplane", enabled = true, tombstone = false, serviceAccountId = serviceAccountId)
      every { dataplaneGroupRepository.findById(dataplaneGroupId) } returns
        Optional.of(DataplaneGroup(id = dataplaneGroupId, organizationId = orgId.value, name = "region", enabled = true, tombstone = false))
      every { featureFlagClient.boolVariation(EnableDefaultSecretStorage, any()) } returns true
      every { secretStorageRepository.listByScopeTypeAndScopeId(SecretStorageScopeType.ORGANIZATION, orgId.value) } returns listOf(orgStorage)
    }

    @Test
    fun `a region with no workspaces uses the organization's storage`() {
      givenWorkspaces()

      assertEquals(listOf(orgStorage), service.listForDataplane(serviceAccountId))
    }

    @Test
    fun `returns each workspace's storage once`() {
      val withOwnStorage = UUID.randomUUID()
      val firstOnOrgStorage = UUID.randomUUID()
      val secondOnOrgStorage = UUID.randomUUID()
      val workspaceStorage = storage(SecretStorageScopeType.WORKSPACE, withOwnStorage)
      givenWorkspaces(withOwnStorage, firstOnOrgStorage, secondOnOrgStorage)
      every { secretStorageRepository.listByScopeTypeAndScopeId(SecretStorageScopeType.WORKSPACE, withOwnStorage) } returns listOf(workspaceStorage)
      every { secretStorageRepository.listByScopeTypeAndScopeId(SecretStorageScopeType.WORKSPACE, firstOnOrgStorage) } returns emptyList()
      every { secretStorageRepository.listByScopeTypeAndScopeId(SecretStorageScopeType.WORKSPACE, secondOnOrgStorage) } returns emptyList()

      assertEquals(listOf(workspaceStorage, orgStorage), service.listForDataplane(serviceAccountId))
    }

    @Test
    fun `a customer's region leaves out the deployment default`() {
      val workspaceWithoutStorage = UUID.randomUUID()
      givenWorkspaces(workspaceWithoutStorage)
      every { secretStorageRepository.listByScopeTypeAndScopeId(SecretStorageScopeType.WORKSPACE, workspaceWithoutStorage) } returns emptyList()
      every { featureFlagClient.boolVariation(EnableDefaultSecretStorage, any()) } returns false
      every { featureFlagClient.boolVariation(UseRuntimeSecretPersistence, any()) } returns false
      every { secretStorageRepository.findById(SecretStorage.DEFAULT_SECRET_STORAGE_ID) } returns defaultStorage

      assertEquals(emptyList<SecretStorage>(), service.listForDataplane(serviceAccountId))
    }

    @Test
    fun `an Airbyte-managed region keeps the deployment default`() {
      every { dataplaneGroupRepository.findById(dataplaneGroupId) } returns
        Optional.of(
          DataplaneGroup(id = dataplaneGroupId, organizationId = DEFAULT_ORGANIZATION_ID, name = "region", enabled = true, tombstone = false),
        )
      every { workspaceRepository.findByOrganizationIdAndDataplaneGroupIdAndTombstoneFalse(DEFAULT_ORGANIZATION_ID, dataplaneGroupId) } returns
        emptyList()
      every { secretStorageRepository.listByScopeTypeAndScopeId(SecretStorageScopeType.ORGANIZATION, DEFAULT_ORGANIZATION_ID) } returns emptyList()
      every { secretStorageRepository.findById(SecretStorage.DEFAULT_SECRET_STORAGE_ID) } returns defaultStorage

      assertEquals(listOf(defaultStorage), service.listForDataplane(serviceAccountId))
    }

    @Test
    fun `leaves out a workspace on the legacy runtime persistence`() {
      val legacyWorkspace = UUID.randomUUID()
      givenWorkspaces(legacyWorkspace)
      every { secretStorageRepository.listByScopeTypeAndScopeId(SecretStorageScopeType.WORKSPACE, legacyWorkspace) } returns emptyList()
      every { featureFlagClient.boolVariation(EnableDefaultSecretStorage, any()) } returns false
      every { featureFlagClient.boolVariation(UseRuntimeSecretPersistence, any()) } returns true

      assertEquals(emptyList<SecretStorage>(), service.listForDataplane(serviceAccountId))
    }

    @Test
    fun `an unknown service account is not found`() {
      val unknown = UUID.randomUUID()
      every { dataplaneRepository.findByServiceAccountId(unknown) } returns null

      assertThrows<ResourceNotFoundProblem> { service.listForDataplane(unknown) }
    }

    @Test
    fun `a data plane whose region is missing is not found`() {
      every { dataplaneGroupRepository.findById(dataplaneGroupId) } returns Optional.empty()

      assertThrows<ResourceNotFoundProblem> { service.listForDataplane(serviceAccountId) }
    }

    private fun givenWorkspaces(vararg workspaceIds: UUID) {
      every { workspaceRepository.findByOrganizationIdAndDataplaneGroupIdAndTombstoneFalse(orgId.value, dataplaneGroupId) } returns
        workspaceIds.map {
          Workspace(
            id = it,
            name = "workspace",
            slug = "workspace",
            dataplaneGroupId = dataplaneGroupId,
            organizationId = orgId.value,
          )
        }
      workspaceIds.forEach { every { organizationRepository.getOrganizationForWorkspaceId(it) } returns Optional.of(org) }
    }

    private fun storage(
      scopeType: SecretStorageScopeType,
      scopeId: UUID,
    ) = SecretStorage(
      id = SecretStorageId(UUID.randomUUID()),
      scopeType = scopeType,
      scopeId = scopeId,
      descriptor = "store",
      storageType = SecretStorageType.AWS_SECRETS_MANAGER,
      configuredFromEnvironment = false,
      createdBy = userId.value,
      updatedBy = userId.value,
      createdAt = null,
      updatedAt = null,
    )
  }

  @Nested
  inner class GetNamesForOrganization {
    private val otherOrgId = UUID.randomUUID()
    private val otherOrg =
      mockk<Organization>().also {
        every { it.organizationId } returns otherOrgId
      }

    @Test
    fun `returns the names of the organization's stores`() {
      val orgStorage = storage(SecretStorageScopeType.ORGANIZATION, orgId.value, "Org store")
      val workspaceStorage = storage(SecretStorageScopeType.WORKSPACE, workspaceId.value, "Finance store")
      every { organizationRepository.getOrganizationForWorkspaceId(workspaceId.value) } returns Optional.of(org)

      assertEquals(
        mapOf(orgStorage.id to "Org store", workspaceStorage.id to "Finance store"),
        service.getNamesForOrganization(orgId, setOf(orgStorage.id, workspaceStorage.id)),
      )
    }

    @Test
    fun `returns the name of the deployment default store`() {
      val defaultStorage =
        storage(SecretStorageScopeType.ORGANIZATION, UUID.randomUUID(), "Default Secret Storage", SecretStorage.DEFAULT_SECRET_STORAGE_ID)

      assertEquals(
        mapOf(defaultStorage.id to "Default Secret Storage"),
        service.getNamesForOrganization(orgId, setOf(defaultStorage.id)),
      )
    }

    @Test
    fun `leaves out stores of another organization`() {
      val otherOrgStorage = storage(SecretStorageScopeType.ORGANIZATION, otherOrgId, "Other org store")
      val otherWorkspaceId = UUID.randomUUID()
      val otherWorkspaceStorage = storage(SecretStorageScopeType.WORKSPACE, otherWorkspaceId, "Other workspace store")
      every { organizationRepository.getOrganizationForWorkspaceId(otherWorkspaceId) } returns Optional.of(otherOrg)

      assertEquals(
        emptyMap<SecretStorageId, String>(),
        service.getNamesForOrganization(orgId, setOf(otherOrgStorage.id, otherWorkspaceStorage.id)),
      )
    }

    @Test
    fun `leaves out deleted and unknown stores`() {
      val deleted = storage(SecretStorageScopeType.ORGANIZATION, orgId.value, "Deleted store", tombstone = true)
      val unknown = SecretStorageId(UUID.randomUUID())
      every { secretStorageRepository.findById(unknown) } returns null

      assertEquals(emptyMap<SecretStorageId, String>(), service.getNamesForOrganization(orgId, setOf(deleted.id, unknown)))
    }

    @Test
    fun `returns nothing for no stores`() {
      assertEquals(emptyMap<SecretStorageId, String>(), service.getNamesForOrganization(orgId, emptySet()))
    }

    private fun storage(
      scopeType: SecretStorageScopeType,
      scopeId: UUID,
      descriptor: String,
      id: SecretStorageId = SecretStorageId(UUID.randomUUID()),
      tombstone: Boolean = false,
    ) = SecretStorage(
      id = id,
      scopeType = scopeType,
      scopeId = scopeId,
      descriptor = descriptor,
      storageType = SecretStorageType.AWS_SECRETS_MANAGER,
      configuredFromEnvironment = false,
      tombstone = tombstone,
      createdBy = userId.value,
      updatedBy = userId.value,
      createdAt = null,
      updatedAt = null,
    ).also { every { secretStorageRepository.findById(it.id) } returns it }
  }

  @Nested
  inner class HydrateStorageConfig {
    @Test
    fun `should throw UnsupportedOperationException if secret storage is configured from environment`() {
      val secretStorage = mockk<SecretStorage>()
      every { secretStorage.configuredFromEnvironment } returns true

      assertThrows<UnsupportedOperationException> { service.hydrateStorageConfig(secretStorage) }
    }

    @Test
    fun `should throw ResourceNotFoundProblem if no secret references found`() {
      val secretStorage = mockk<SecretStorage>()
      every { secretStorage.configuredFromEnvironment } returns false
      every { secretStorage.id } returns secretStorageId
      every {
        secretReferenceRepository.listByScopeTypeAndScopeId(SecretReferenceScopeType.SECRET_STORAGE, secretStorageId.value)
      } returns emptyList()

      assertThrows<ResourceNotFoundProblem> { service.hydrateStorageConfig(secretStorage) }
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

      assertThrows<IllegalStateException> { service.hydrateStorageConfig(secretStorage) }
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

      assertThrows<ResourceNotFoundProblem> { service.hydrateStorageConfig(secretStorage) }
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
      every { secretsRepositoryReader.fetchJsonSecretFromDefaultSecretPersistence(any()) } returns fetchedConfig

      val result = service.hydrateStorageConfig(secretStorage)
      assertEquals(SecretStorageWithConfig(secretStorage, fetchedConfig), result)
    }
  }
}
