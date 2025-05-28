/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.secrets

import io.airbyte.commons.json.Jsons
import io.airbyte.config.secrets.ConfigWithProcessedSecrets
import io.airbyte.config.secrets.ProcessedSecretNode
import io.airbyte.config.secrets.SecretCoordinate
import io.airbyte.config.secrets.SecretReferenceConfig
import io.airbyte.data.services.SecretConfigService
import io.airbyte.data.services.SecretReferenceService
import io.airbyte.domain.models.ActorId
import io.airbyte.domain.models.SecretConfig
import io.airbyte.domain.models.SecretConfigCreate
import io.airbyte.domain.models.SecretConfigId
import io.airbyte.domain.models.SecretReference
import io.airbyte.domain.models.SecretReferenceCreate
import io.airbyte.domain.models.SecretReferenceId
import io.airbyte.domain.models.SecretReferenceScopeType
import io.airbyte.domain.models.SecretReferenceWithConfig
import io.airbyte.domain.models.SecretStorageId
import io.airbyte.domain.models.UserId
import io.airbyte.domain.models.WorkspaceId
import io.airbyte.featureflag.PersistSecretConfigsAndReferences
import io.airbyte.featureflag.ReadSecretReferenceIdsInConfigs
import io.airbyte.featureflag.TestClient
import io.airbyte.persistence.job.WorkspaceHelper
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.UUID

class SecretReferenceServiceTest {
  private val secretReferenceRepository = mockk<SecretReferenceService>()
  private val secretConfigRepository = mockk<SecretConfigService>()
  private val workspaceHelper = mockk<WorkspaceHelper>()
  private val featureFlagClient = mockk<TestClient>()
  private val secretReferenceService = SecretReferenceService(secretReferenceRepository, secretConfigRepository, featureFlagClient, workspaceHelper)

  @Nested
  inner class GetConfigWithSecretReferences {
    @Test
    fun `gets config with referenced secret map`() {
      val actorId = UUID.randomUUID()
      val storageId = UUID.randomUUID()
      val workspaceId = UUID.randomUUID()

      val urlRefId = UUID.randomUUID()
      val apiKeyRefId = UUID.randomUUID()

      val jsonConfig =
        Jsons.jsonNode(
          mapOf(
            "username" to "bob",
            "secretUrl" to mapOf("_secret_reference_id" to urlRefId.toString()),
            "auth" to
              mapOf(
                "password" to mapOf("_secret" to "airbyte_secret_coordinate_v1"),
                "apiKey" to mapOf("_secret_reference_id" to apiKeyRefId.toString()),
              ),
          ),
        )

      val secretUrlRef = mockk<SecretReferenceWithConfig>()
      every { secretUrlRef.secretReference.id } returns SecretReferenceId(urlRefId)
      every { secretUrlRef.secretReference.hydrationPath } returns "$.secretUrl"
      every { secretUrlRef.secretConfig.secretStorageId } returns storageId
      every { secretUrlRef.secretConfig.airbyteManaged } returns true
      every { secretUrlRef.secretConfig.externalCoordinate } returns "url-coord"

      val apiKeyRef = mockk<SecretReferenceWithConfig>()
      every { apiKeyRef.secretReference.id } returns SecretReferenceId(apiKeyRefId)
      every { apiKeyRef.secretReference.hydrationPath } returns "$.auth.apiKey"
      every { apiKeyRef.secretConfig.secretStorageId } returns storageId
      every { apiKeyRef.secretConfig.airbyteManaged } returns false
      every { apiKeyRef.secretConfig.externalCoordinate } returns "auth-key-coord"

      every { workspaceHelper.getOrganizationForWorkspace(any()) } returns workspaceId
      every { featureFlagClient.boolVariation(ReadSecretReferenceIdsInConfigs, any()) } returns true

      every { secretReferenceRepository.listWithConfigByScopeTypeAndScopeId(SecretReferenceScopeType.ACTOR, actorId) } returns
        listOf(
          secretUrlRef,
          apiKeyRef,
        )

      val expectedReferencedSecretsMap =
        mapOf(
          "$.auth.password" to SecretReferenceConfig(SecretCoordinate.AirbyteManagedSecretCoordinate("airbyte_secret_coordinate"), null),
          "$.secretUrl" to SecretReferenceConfig(SecretCoordinate.ExternalSecretCoordinate("url-coord"), storageId, urlRefId),
          "$.auth.apiKey" to SecretReferenceConfig(SecretCoordinate.ExternalSecretCoordinate("auth-key-coord"), storageId, apiKeyRefId),
        )

      val actual = secretReferenceService.getConfigWithSecretReferences(ActorId(actorId), jsonConfig, WorkspaceId(workspaceId))
      assertEquals(expectedReferencedSecretsMap, actual.referencedSecrets)
    }

    @Test
    fun `applies non-persisted secret refs over persisted secret refs`() {
      val actorId = UUID.randomUUID()
      val storageId = UUID.randomUUID()
      val workspaceId = UUID.randomUUID()

      val urlRefId = UUID.randomUUID()
      val apiKeyRefId = UUID.randomUUID()

      val jsonConfig =
        Jsons.jsonNode(
          mapOf(
            "username" to "bob",
            "secretUrl" to mapOf("_secret_reference_id" to urlRefId, "_secret" to "url-coord", "_secret_storage_id" to storageId),
            "auth" to
              mapOf(
                "password" to mapOf("_secret" to "airbyte_secret_coordinate_v1", "_secret_storage_id" to storageId), // not persisted, no reference ID
                "apiKey" to mapOf("_secret_reference_id" to apiKeyRefId.toString()),
              ),
          ),
        )

      val persistedSecretUrlRef = mockk<SecretReferenceWithConfig>()
      every { persistedSecretUrlRef.secretReference.id } returns SecretReferenceId(urlRefId)
      every { persistedSecretUrlRef.secretReference.hydrationPath } returns "$.secretUrl"
      every { persistedSecretUrlRef.secretConfig.secretStorageId } returns storageId
      every { persistedSecretUrlRef.secretConfig.airbyteManaged } returns false
      every { persistedSecretUrlRef.secretConfig.externalCoordinate } returns "url-coord"

      val persistedApiKeyRef = mockk<SecretReferenceWithConfig>()
      every { persistedApiKeyRef.secretReference.id } returns SecretReferenceId(apiKeyRefId)
      every { persistedApiKeyRef.secretReference.hydrationPath } returns "$.auth.apiKey"
      every { persistedApiKeyRef.secretConfig.secretStorageId } returns storageId
      every { persistedApiKeyRef.secretConfig.airbyteManaged } returns false
      every { persistedApiKeyRef.secretConfig.externalCoordinate } returns "auth-key-coord"
      every { workspaceHelper.getOrganizationForWorkspace(any()) } returns workspaceId
      every { featureFlagClient.boolVariation(ReadSecretReferenceIdsInConfigs, any()) } returns true

      every { secretReferenceRepository.listWithConfigByScopeTypeAndScopeId(SecretReferenceScopeType.ACTOR, actorId) } returns
        listOf(
          persistedSecretUrlRef,
          persistedApiKeyRef,
        )

      val expectedReferencedSecretsMap =
        mapOf(
          "$.auth.password" to SecretReferenceConfig(SecretCoordinate.AirbyteManagedSecretCoordinate("airbyte_secret_coordinate"), storageId, null),
          "$.secretUrl" to SecretReferenceConfig(SecretCoordinate.ExternalSecretCoordinate("url-coord"), storageId, urlRefId),
          "$.auth.apiKey" to SecretReferenceConfig(SecretCoordinate.ExternalSecretCoordinate("auth-key-coord"), storageId, apiKeyRefId),
        )

      val actual = secretReferenceService.getConfigWithSecretReferences(ActorId(actorId), jsonConfig, WorkspaceId(workspaceId))
      assertEquals(expectedReferencedSecretsMap, actual.referencedSecrets)
    }
  }

  @Test
  fun `applies raw values over persisted secret refs`() {
    val actorId = UUID.randomUUID()
    val storageId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()

    val clientSecretRefId = UUID.randomUUID()
    val accessTokenRefId = UUID.randomUUID()

    val jsonConfig =
      Jsons.jsonNode(
        mapOf(
          "username" to "bob",
          "auth" to
            mapOf(
              "clientSecret" to "my-client-secret", // raw value coming from oauth cred injection
              "accessToken" to mapOf("_secret_reference_id" to accessTokenRefId.toString()),
            ),
        ),
      )

    val clientSecretRef =
      mockk<SecretReferenceWithConfig> {
        every { secretReference.id } returns SecretReferenceId(clientSecretRefId)
        every { secretReference.hydrationPath } returns "$.auth.clientSecret"
        every { secretConfig.secretStorageId } returns storageId
        every { secretConfig.airbyteManaged } returns true
        every { secretConfig.externalCoordinate } returns SecretCoordinate.AirbyteManagedSecretCoordinate().fullCoordinate
      }

    val accessTokenCoord = SecretCoordinate.AirbyteManagedSecretCoordinate()
    val accessTokenRef =
      mockk<SecretReferenceWithConfig> {
        every { secretReference.id } returns SecretReferenceId(accessTokenRefId)
        every { secretReference.hydrationPath } returns "$.auth.accessToken"
        every { secretConfig.secretStorageId } returns storageId
        every { secretConfig.airbyteManaged } returns true
        every { secretConfig.externalCoordinate } returns accessTokenCoord.fullCoordinate
      }

    every { workspaceHelper.getOrganizationForWorkspace(any()) } returns workspaceId
    every { featureFlagClient.boolVariation(ReadSecretReferenceIdsInConfigs, any()) } returns true

    every { secretReferenceRepository.listWithConfigByScopeTypeAndScopeId(SecretReferenceScopeType.ACTOR, actorId) } returns
      listOf(
        clientSecretRef,
        accessTokenRef,
      )

    val expectedReferencedSecretsMap =
      mapOf(
        "$.auth.accessToken" to SecretReferenceConfig(accessTokenCoord, storageId, accessTokenRefId),
      )

    val actual = secretReferenceService.getConfigWithSecretReferences(ActorId(actorId), jsonConfig, WorkspaceId(workspaceId))
    assertEquals(expectedReferencedSecretsMap, actual.referencedSecrets)
  }

  @Nested
  inner class CreateSecretConfigAndReference {
    private val secretStorageId = SecretStorageId(UUID.randomUUID())
    private val actorId = ActorId(UUID.randomUUID())
    private val currentUserId = UserId(UUID.randomUUID())
    private val airbyteManagedPasswordCoordinate = SecretCoordinate.AirbyteManagedSecretCoordinate().fullCoordinate

    @Test
    fun `creates secret configs and reference with new config`() {
      every { secretConfigRepository.findByStorageIdAndExternalCoordinate(secretStorageId, airbyteManagedPasswordCoordinate) } returns null

      val createConfigSlot = slot<SecretConfigCreate>()
      val createRefSlot = slot<SecretReferenceCreate>()

      val createdPasswordConfig =
        mockk<SecretConfig> {
          every { id } returns SecretConfigId(UUID.randomUUID())
        }
      val createdPasswordRef =
        mockk<SecretReference> {
          every { id } returns SecretReferenceId(UUID.randomUUID())
        }

      every { secretConfigRepository.create(capture(createConfigSlot)) } returns createdPasswordConfig
      every { secretReferenceRepository.createAndReplace(capture(createRefSlot)) } returns createdPasswordRef

      val result =
        secretReferenceService.createSecretConfigAndReference(
          secretStorageId,
          airbyteManagedPasswordCoordinate,
          true,
          currentUserId,
          "$.password",
          SecretReferenceScopeType.ACTOR,
          actorId.value,
        )

      result shouldBe createdPasswordRef.id

      createConfigSlot.captured shouldBe
        SecretConfigCreate(
          secretStorageId = secretStorageId,
          descriptor = airbyteManagedPasswordCoordinate,
          externalCoordinate = airbyteManagedPasswordCoordinate,
          airbyteManaged = true,
          createdBy = currentUserId,
        )

      createRefSlot.captured shouldBe
        SecretReferenceCreate(
          secretConfigId = createdPasswordConfig.id,
          hydrationPath = "$.password",
          scopeType = SecretReferenceScopeType.ACTOR,
          scopeId = actorId.value,
        )
    }

    @Test
    fun `creates secret configs and reference with existing config`() {
      val expectedConfigId = SecretConfigId(UUID.randomUUID())
      every { secretConfigRepository.findByStorageIdAndExternalCoordinate(secretStorageId, airbyteManagedPasswordCoordinate) } returns
        mockk<SecretConfig> {
          every { id } returns expectedConfigId
        }

      val createRefSlot = slot<SecretReferenceCreate>()

      val createdPasswordRef =
        mockk<SecretReference> {
          every { id } returns SecretReferenceId(UUID.randomUUID())
        }

      every { secretReferenceRepository.createAndReplace(capture(createRefSlot)) } returns createdPasswordRef

      val result =
        secretReferenceService.createSecretConfigAndReference(
          secretStorageId,
          airbyteManagedPasswordCoordinate,
          true,
          currentUserId,
          "$.password",
          SecretReferenceScopeType.ACTOR,
          actorId.value,
        )

      result shouldBe createdPasswordRef.id
      createRefSlot.captured.secretConfigId shouldBe expectedConfigId

      verify(exactly = 0) {
        secretConfigRepository.create(any())
      }
    }
  }

  @Nested
  inner class CreateAndInsertSecretReferencesWithStorageId {
    private val secretStorageId = SecretStorageId(UUID.randomUUID())
    private val actorId = ActorId(UUID.randomUUID())
    private val workspaceId = WorkspaceId(UUID.randomUUID())
    private val currentUserId = UserId(UUID.randomUUID())
    private val airbyteManagedPasswordCoordinate = "airbyte_managed_password_v1"
    private val externalClientSecretCoordinate = "some.external.client.secret.coordinate"

    @Test
    fun `creates secret configs and references when flag enabled`() {
      every { featureFlagClient.boolVariation(PersistSecretConfigsAndReferences, any()) } returns true
      every { workspaceHelper.getOrganizationForWorkspace(any()) } returns workspaceId.value

      val inputActorConfig =
        ConfigWithProcessedSecrets(
          Jsons.jsonNode(
            mapOf(
              "username" to "bob",
              "auth" to
                mapOf(
                  "clientSecret" to mapOf("_secret" to externalClientSecretCoordinate),
                ),
              "password" to mapOf("_secret" to airbyteManagedPasswordCoordinate),
            ),
          ),
          mapOf(
            "$.password" to ProcessedSecretNode(SecretCoordinate.AirbyteManagedSecretCoordinate.fromFullCoordinate(airbyteManagedPasswordCoordinate)),
            "$.auth.clientSecret" to ProcessedSecretNode(SecretCoordinate.ExternalSecretCoordinate(externalClientSecretCoordinate)),
          ),
        )

      // mock existing secret references for paths that are no longer present in the config.
      // these are expected to be deleted.
      every {
        secretReferenceRepository.listByScopeTypeAndScopeId(
          SecretReferenceScopeType.ACTOR,
          actorId.value,
        )
      } returns
        listOf(
          mockk<SecretReference> {
            every { hydrationPath } returns "$.old.password"
            every { id } returns SecretReferenceId(UUID.randomUUID())
          },
          mockk<SecretReference> {
            every { hydrationPath } returns "$.old.auth.clientSecret"
            every { id } returns SecretReferenceId(UUID.randomUUID())
          },
        )

      every { secretReferenceRepository.deleteByScopeTypeAndScopeIdAndHydrationPath(any(), any(), any()) } returns Unit
      every { secretConfigRepository.findByStorageIdAndExternalCoordinate(secretStorageId, airbyteManagedPasswordCoordinate) } returns null
      every { secretConfigRepository.findByStorageIdAndExternalCoordinate(secretStorageId, externalClientSecretCoordinate) } returns null

      val createConfigSlot = slot<SecretConfigCreate>()
      val createRefSlot = slot<SecretReferenceCreate>()

      val createdPasswordConfig =
        mockk<SecretConfig> {
          every { id } returns SecretConfigId(UUID.randomUUID())
        }
      val createdPasswordRef =
        mockk<SecretReference> {
          every { id } returns SecretReferenceId(UUID.randomUUID())
        }
      val createdClientSecretConfig =
        mockk<SecretConfig> {
          every { id } returns SecretConfigId(UUID.randomUUID())
        }
      val createdClientSecretRef =
        mockk<SecretReference> {
          every { id } returns SecretReferenceId(UUID.randomUUID())
        }

      every { secretConfigRepository.create(capture(createConfigSlot)) } returns createdPasswordConfig andThen createdClientSecretConfig
      every { secretReferenceRepository.createAndReplace(capture(createRefSlot)) } returns createdPasswordRef andThen createdClientSecretRef

      val result =
        secretReferenceService.createAndInsertSecretReferencesWithStorageId(
          inputActorConfig,
          actorId,
          workspaceId,
          secretStorageId,
          currentUserId,
        )

      result.value shouldBe
        Jsons.jsonNode(
          mapOf(
            "username" to "bob",
            "auth" to
              mapOf(
                "clientSecret" to
                  mapOf("_secret_reference_id" to createdClientSecretRef.id.value.toString(), "_secret" to externalClientSecretCoordinate),
              ),
            "password" to mapOf("_secret_reference_id" to createdPasswordRef.id.value.toString(), "_secret" to airbyteManagedPasswordCoordinate),
          ),
        )
      // any existing references that are no longer present in the config should be deleted
      verify {
        secretReferenceRepository.deleteByScopeTypeAndScopeIdAndHydrationPath(
          SecretReferenceScopeType.ACTOR,
          actorId.value,
          "$.old.password",
        )
      }
      verify {
        secretReferenceRepository.deleteByScopeTypeAndScopeIdAndHydrationPath(
          SecretReferenceScopeType.ACTOR,
          actorId.value,
          "$.old.auth.clientSecret",
        )
      }
    }

    @Test
    fun `does not create secret configs and references when flag disabled`() {
      every { featureFlagClient.boolVariation(PersistSecretConfigsAndReferences, any()) } returns false
      every { workspaceHelper.getOrganizationForWorkspace(any()) } returns UUID.randomUUID()

      val inputActorConfig =
        ConfigWithProcessedSecrets(
          Jsons.jsonNode(
            mapOf(
              "username" to "bob",
              "auth" to
                mapOf(
                  "clientSecret" to mapOf("_secret" to externalClientSecretCoordinate),
                ),
              "password" to mapOf("_secret" to airbyteManagedPasswordCoordinate),
            ),
          ),
          mapOf(
            "$.password" to ProcessedSecretNode(SecretCoordinate.AirbyteManagedSecretCoordinate.fromFullCoordinate(airbyteManagedPasswordCoordinate)),
            "$.auth.clientSecret" to ProcessedSecretNode(SecretCoordinate.ExternalSecretCoordinate(externalClientSecretCoordinate)),
          ),
        )

      val result =
        secretReferenceService.createAndInsertSecretReferencesWithStorageId(
          inputActorConfig,
          actorId,
          workspaceId,
          secretStorageId,
          currentUserId,
        )

      result.value shouldBe inputActorConfig.originalConfig
      verify(exactly = 0) {
        secretConfigRepository.create(any())
        secretReferenceRepository.createAndReplace(any())
      }
    }
  }
}
