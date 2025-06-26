/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.bootloader

import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.commons.DEFAULT_USER_ID
import io.airbyte.config.secrets.persistence.SecretPersistence.ImplementationTypes
import io.airbyte.data.services.SecretStorageService
import io.airbyte.domain.models.PatchField.Companion.toPatch
import io.airbyte.domain.models.SecretStorage
import io.airbyte.domain.models.SecretStorageCreate
import io.airbyte.domain.models.SecretStorageScopeType
import io.airbyte.domain.models.SecretStorageType
import io.airbyte.domain.models.UserId
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Property
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

@Singleton
class SecretStorageInitializer(
  private val secretStorageService: SecretStorageService,
  @Property(name = "airbyte.secret.persistence") private val configuredSecretPersistenceType: String,
) {
  companion object {
    private const val DEFAULT_SECRET_STORAGE_DESCRIPTOR = "Default Secret Storage"
  }

  /**
   * Creates or updates the default secret storage for the instance. The default secret storage is
   * always set with configuredFromEnvironment = true, and is associated with the default
   * organization and user.
   *
   * If the default secret storage already exists, it will be updated to match the configured
   * secret storage type if it differs from the existing one.
   */
  fun createOrUpdateDefaultSecretStorage() {
    val configuredSecretStorageType = mapConfiguredSecretPersistenceType(configuredSecretPersistenceType)

    when (val existingStorage = secretStorageService.findById(SecretStorage.DEFAULT_SECRET_STORAGE_ID)) {
      null -> {
        logger.info { "Creating default secret storage." }
        secretStorageService.create(
          SecretStorageCreate(
            id = SecretStorage.DEFAULT_SECRET_STORAGE_ID,
            scopeType = SecretStorageScopeType.ORGANIZATION,
            scopeId = DEFAULT_ORGANIZATION_ID,
            descriptor = DEFAULT_SECRET_STORAGE_DESCRIPTOR,
            storageType = configuredSecretStorageType,
            configuredFromEnvironment = true,
            createdBy = UserId(DEFAULT_USER_ID),
          ),
        )
      }
      else -> {
        logger.info { "Default secret storage already exists." }
        if (existingStorage.storageType != configuredSecretStorageType) {
          logger.info {
            "Existing secret storage type ${existingStorage.storageType} does not match configured secret storage type $configuredSecretPersistenceType. Updating..."
          }
          secretStorageService.patch(
            id = existingStorage.id,
            updatedBy = UserId(DEFAULT_USER_ID),
            storageType = configuredSecretStorageType.toPatch(),
          )
        }
      }
    }
  }

  /**
   * Maps the configured secret persistence type to the corresponding enum value if one exists.
   *
   * Note that this mapping is not crucial for existing functionality, since the default secret
   * storage is going to be configured entirely from its environment anyway. In the future, this
   * mapping may become more important especially if we want to support multiple
   * environment-configured secret storage types within the same Airbyte instance.
   */
  private fun mapConfiguredSecretPersistenceType(configuredType: String): SecretStorageType =
    when (configuredType.lowercase()) {
      ImplementationTypes.AWS_SECRET_MANAGER -> SecretStorageType.AWS_SECRETS_MANAGER
      ImplementationTypes.GOOGLE_SECRET_MANAGER -> SecretStorageType.GOOGLE_SECRET_MANAGER
      ImplementationTypes.VAULT -> SecretStorageType.VAULT
      ImplementationTypes.AZURE_KEY_VAULT -> SecretStorageType.AZURE_KEY_VAULT
      ImplementationTypes.TESTING_CONFIG_DB_TABLE -> SecretStorageType.LOCAL_TESTING
      else -> {
        logger.warn { "Unknown secret storage type: $configuredType. Defaulting to local testing." }
        SecretStorageType.LOCAL_TESTING
      }
    }
}
