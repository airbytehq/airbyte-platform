/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.secrets

import io.airbyte.commons.json.Jsons
import io.airbyte.config.secrets.SecretCoordinate
import io.airbyte.config.secrets.SecretsRepositoryWriter
import io.airbyte.domain.models.SecretReferenceId
import io.airbyte.domain.models.SecretReferenceScopeType
import io.airbyte.domain.models.SecretStorage
import io.airbyte.domain.models.SecretStorageCreate
import io.airbyte.domain.models.SecretStorageId
import io.airbyte.domain.models.SecretStorageScopeType
import io.airbyte.domain.models.SecretStorageType
import io.airbyte.domain.models.UserId
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import java.util.UUID

class SecretStorageCredentialsServiceTest {
  private val userId = UserId(UUID.randomUUID())
  private val secretStorageId = SecretStorageId(UUID.randomUUID())
  private val orgId = UUID.randomUUID()

  private val secretsRepositoryWriter: SecretsRepositoryWriter = mockk()
  private val secretReferenceService: SecretReferenceService = mockk()

  private val service =
    SecretStorageCredentialsService(
      secretsRepositoryWriter,
      secretReferenceService,
    )

  @Test
  fun `should write storage credentials when config is provided`() {
    val secretStorageCreate =
      SecretStorageCreate(
        scopeType = SecretStorageScopeType.ORGANIZATION,
        scopeId = orgId,
        descriptor = "descriptor",
        storageType = SecretStorageType.AWS_SECRETS_MANAGER,
        configuredFromEnvironment = false,
        createdBy = userId,
      )
    val storageConfig = Jsons.jsonNode(mapOf("key" to "value"))

    val newCoordinate = SecretCoordinate.AirbyteManagedSecretCoordinate()
    every { secretsRepositoryWriter.storeInDefaultPersistence(any(), eq(Jsons.serialize(storageConfig))) } returns newCoordinate

    val newSecretRefId = SecretReferenceId(UUID.randomUUID())
    every {
      secretReferenceService.createSecretConfigAndReference(
        SecretStorage.DEFAULT_SECRET_STORAGE_ID,
        externalCoordinate = newCoordinate.fullCoordinate,
        airbyteManaged = true,
        currentUserId = userId,
        scopeType = SecretReferenceScopeType.SECRET_STORAGE,
        scopeId = secretStorageId.value,
        hydrationPath = null,
      )
    } returns newSecretRefId

    service.writeStorageCredentials(secretStorageCreate, storageConfig, secretStorageId, userId)

    verify {
      secretsRepositoryWriter.storeInDefaultPersistence(any(), eq(Jsons.serialize(storageConfig)))
      secretReferenceService.createSecretConfigAndReference(
        SecretStorage.DEFAULT_SECRET_STORAGE_ID,
        externalCoordinate = newCoordinate.fullCoordinate,
        airbyteManaged = true,
        currentUserId = userId,
        scopeType = SecretReferenceScopeType.SECRET_STORAGE,
        scopeId = secretStorageId.value,
        hydrationPath = null,
      )
    }
  }

  @Test
  fun `should not write credentials when config is null`() {
    val secretStorageCreate =
      SecretStorageCreate(
        scopeType = SecretStorageScopeType.ORGANIZATION,
        scopeId = orgId,
        descriptor = "descriptor",
        storageType = SecretStorageType.AWS_SECRETS_MANAGER,
        configuredFromEnvironment = true,
        createdBy = userId,
      )

    service.writeStorageCredentials(secretStorageCreate, null, secretStorageId, userId)

    verify(exactly = 0) {
      secretsRepositoryWriter.storeInDefaultPersistence(any(), any())
      secretReferenceService.createSecretConfigAndReference(
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
      )
    }
  }
}
