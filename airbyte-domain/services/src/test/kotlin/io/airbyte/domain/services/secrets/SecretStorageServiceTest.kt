/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.secrets

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.commons.json.Jsons
import io.airbyte.config.Organization
import io.airbyte.config.secrets.SecretsRepositoryReader
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
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.string.shouldNotContain
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
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

  private val service =
    SecretStorageService(
      secretStorageRepository,
      organizationRepository,
      secretReferenceRepository,
      secretsRepositoryReader,
      secretConfigService,
      featureFlagClient,
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

      service.createSecretStorage(secretStorageCreate, storageConfig) shouldBe secretStorage

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

      service.createSecretStorage(secretStorageCreate, null) shouldBe secretStorage

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

      shouldThrow<IllegalArgumentException> {
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

      service.createSecretStorage(create, Jsons.jsonNode(validAzureConfig + ("extraKey" to "allowed"))) shouldBe secretStorage
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
        shouldThrow<BadRequestProblem> {
          service.createSecretStorage(create, wrappedConfig)
        }
      problem.problem.getData().toString() shouldContain "configuration"
      problem.problem.getData().toString() shouldNotContain "super-secret-value"
    }

    @Test
    fun `should reject config that is not a JSON object`() {
      val create = secretStorageCreate(SecretStorageType.AZURE_KEY_VAULT)

      shouldThrow<BadRequestProblem> {
        service.createSecretStorage(create, Jsons.jsonNode("just a string"))
      }
    }

    @Test
    fun `should reject config with non-string scalar values`() {
      val create = secretStorageCreate(SecretStorageType.AZURE_KEY_VAULT)
      val config = Jsons.jsonNode(validAzureConfig + ("someFlag" to true))

      val problem =
        shouldThrow<BadRequestProblem> {
          service.createSecretStorage(create, config)
        }
      problem.problem.getData().toString() shouldContain "someFlag"
    }

    @Test
    fun `should reject azure config with missing required keys`() {
      val create = secretStorageCreate(SecretStorageType.AZURE_KEY_VAULT)
      val config = Jsons.jsonNode(validAzureConfig - "tenantId" - "clientSecret")

      val problem =
        shouldThrow<BadRequestProblem> {
          service.createSecretStorage(create, config)
        }
      problem.problem.getData().toString() shouldContain "clientSecret"
      problem.problem.getData().toString() shouldContain "tenantId"
    }

    @Test
    fun `should create secret storage with valid vault config`() {
      val create = secretStorageCreate(SecretStorageType.VAULT)
      val secretStorage = mockk<SecretStorage>()
      every { secretStorageRepository.create(create) } returns secretStorage

      val config = Jsons.jsonNode(mapOf("address" to "http://vault:8200", "token" to "vault-token", "prefix" to "abc"))

      service.createSecretStorage(create, config) shouldBe secretStorage
    }

    @Test
    fun `should create vault storage with blank prefix`() {
      val create = secretStorageCreate(SecretStorageType.VAULT)
      val secretStorage = mockk<SecretStorage>()
      every { secretStorageRepository.create(create) } returns secretStorage

      val config = Jsons.jsonNode(mapOf("address" to "http://vault:8200", "token" to "vault-token", "prefix" to ""))

      service.createSecretStorage(create, config) shouldBe secretStorage
    }

    @Test
    fun `should reject vault config without prefix key`() {
      val create = secretStorageCreate(SecretStorageType.VAULT)
      val config = Jsons.jsonNode(mapOf("address" to "http://vault:8200", "token" to "vault-token"))

      val problem =
        shouldThrow<BadRequestProblem> {
          service.createSecretStorage(create, config)
        }
      problem.problem.getData().toString() shouldContain "prefix"
    }

    @Test
    fun `should reject config with blank required values`() {
      val create = secretStorageCreate(SecretStorageType.VAULT)
      val config = Jsons.jsonNode(mapOf("address" to "http://vault:8200", "token" to "", "prefix" to "abc"))

      val problem =
        shouldThrow<BadRequestProblem> {
          service.createSecretStorage(create, config)
        }
      problem.problem.getData().toString() shouldContain "token"
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

      service.createSecretStorage(create, config) shouldBe secretStorage
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
        shouldThrow<BadRequestProblem> {
          service.createSecretStorage(create, config)
        }
      problem.problem.getData().toString() shouldContain "awsAccessKey"
      problem.problem.getData().toString() shouldContain "awsSecretAccessKey"
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

      service.createSecretStorage(create, config) shouldBe secretStorage
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
        shouldThrow<BadRequestProblem> {
          service.createSecretStorage(create, config)
        }
      problem.problem.getData().toString() shouldContain "externalId"
      problem.problem.getData().toString() shouldContain "roleArn"
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
        shouldThrow<BadRequestProblem> {
          service.createSecretStorage(create, config)
        }
      problem.problem.getData().toString() shouldContain "ACESS_KEY"
      problem.problem.getData().toString() shouldContain "ACCESS_KEY"
      problem.problem.getData().toString() shouldContain "IAM_ROLE"
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

      service.createSecretStorage(create, config) shouldBe secretStorage
    }

    @Test
    fun `should reject google secret manager config with missing required keys`() {
      val create = secretStorageCreate(SecretStorageType.GOOGLE_SECRET_MANAGER)
      val config = Jsons.jsonNode(mapOf("gcpProjectId" to "my-project"))

      val problem =
        shouldThrow<BadRequestProblem> {
          service.createSecretStorage(create, config)
        }
      problem.problem.getData().toString() shouldContain "gcpCredentialsJson"
    }
  }

  @Nested
  inner class Disable {
    @Test
    fun `should disable secret storage`() {
      val secretStorage = mockk<SecretStorage>()
      every { secretStorageRepository.patch(secretStorageId, tombstone = true.toPatch(), updatedBy = userId) } returns secretStorage

      service.deleteSecretStorage(secretStorageId, userId) shouldBe secretStorage

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

      service.getByWorkspaceId(workspaceId) shouldBe secretStorage
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

      service.getByWorkspaceId(workspaceId) shouldBe orgSecretStorage
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

      service.getByWorkspaceId(workspaceId) shouldBe expected
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
      every { secretsRepositoryReader.fetchJsonSecretFromDefaultSecretPersistence(any()) } returns fetchedConfig

      val result = service.hydrateStorageConfig(secretStorage)
      result shouldBe SecretStorageWithConfig(secretStorage, fetchedConfig)
    }
  }
}
