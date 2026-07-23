/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.secrets

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.config.secrets.SecretCoordinate
import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate.Companion.DEFAULT_VERSION
import io.airbyte.config.secrets.SecretsRepositoryWriter
import io.airbyte.domain.models.SecretReferenceScopeType
import io.airbyte.domain.models.SecretStorage.Companion.DEFAULT_SECRET_STORAGE_ID
import io.airbyte.domain.models.SecretStorageCreate
import io.airbyte.domain.models.SecretStorageId
import io.airbyte.domain.models.UserId
import jakarta.inject.Singleton

/**
 * Service for managing credentials associated with secret storages.
 * This service is responsible for storing the configuration/credentials for custom secret storages
 * in the default secret persistence.
 */
@Singleton
class SecretStorageCredentialsService(
  private val secretsRepositoryWriter: SecretsRepositoryWriter,
  private val secretReferenceService: SecretReferenceService,
) {
  /**
   * Writes the credentials for the new secret storage to the default secret persistence.
   * All credentials for custom secret storages are stored in the default secret persistence.
   */
  fun writeStorageCredentials(
    secretStorageCreate: SecretStorageCreate,
    storageConfig: JsonNode?,
    secretStorageId: SecretStorageId,
    currentUserId: UserId?,
  ) {
    if (storageConfig == null) {
      // No config to store
      return
    }

    val secretCoordinate =
      secretsRepositoryWriter.storeInDefaultPersistence(
        SecretCoordinate.AirbyteManagedSecretCoordinate(
          "storage_${secretStorageCreate.scopeType.name.lowercase()}",
          secretStorageCreate.scopeId,
          DEFAULT_VERSION,
        ),
        Jsons.serialize(storageConfig),
      )

    secretReferenceService.createSecretConfigAndReference(
      DEFAULT_SECRET_STORAGE_ID,
      externalCoordinate = secretCoordinate.fullCoordinate,
      airbyteManaged = true,
      currentUserId = currentUserId,
      scopeType = SecretReferenceScopeType.SECRET_STORAGE,
      scopeId = secretStorageId.value,
      hydrationPath = null,
    )
  }
}
