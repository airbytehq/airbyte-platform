/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data.mappers

import io.airbyte.domain.models.SecretStorageCreate
import io.airbyte.domain.models.SecretStorageId
import io.airbyte.data.repositories.entities.SecretStorage as EntitySecretStorage
import io.airbyte.db.instance.configs.jooq.generated.enums.SecretStorageScopeType as EntityScopeType
import io.airbyte.db.instance.configs.jooq.generated.enums.SecretStorageType as EntityStorageType
import io.airbyte.domain.models.SecretStorage as ModelSecretStorage
import io.airbyte.domain.models.SecretStorageScopeType as ModelScopeType
import io.airbyte.domain.models.SecretStorageType as ModelStorageType

object SecretStorageMapper {
  fun EntitySecretStorage.toConfigModel(): ModelSecretStorage {
    val id =
      this.id
        ?: throw IllegalStateException("Cannot map EntitySecretStorage that lacks an id")
    return ModelSecretStorage(
      id = SecretStorageId(id),
      scopeType = this.scopeType.toConfigModel(),
      scopeId = this.scopeId,
      descriptor = this.descriptor,
      storageType = this.storageType.toConfigModel(),
      configuredFromEnvironment = this.configuredFromEnvironment,
      tombstone = this.tombstone,
      createdBy = this.createdBy,
      updatedBy = this.updatedBy,
      createdAt = this.createdAt,
      updatedAt = this.updatedAt,
    )
  }

  fun ModelSecretStorage.toEntity(): EntitySecretStorage =
    EntitySecretStorage(
      id = this.id.value,
      scopeType = this.scopeType.toEntity(),
      scopeId = this.scopeId,
      descriptor = this.descriptor,
      storageType = this.storageType.toEntity(),
      configuredFromEnvironment = this.configuredFromEnvironment,
      tombstone = this.tombstone,
      createdBy = this.createdBy,
      updatedBy = this.updatedBy,
      createdAt = this.createdAt,
      updatedAt = this.updatedAt,
    )

  fun EntityStorageType.toConfigModel(): ModelStorageType =
    when (this) {
      EntityStorageType.aws_secrets_manager -> ModelStorageType.AWS_SECRETS_MANAGER
      EntityStorageType.google_secret_manager -> ModelStorageType.GOOGLE_SECRET_MANAGER
      EntityStorageType.vault -> ModelStorageType.VAULT
      EntityStorageType.azure_key_vault -> ModelStorageType.AZURE_KEY_VAULT
      EntityStorageType.local_testing -> ModelStorageType.LOCAL_TESTING
    }

  fun ModelStorageType.toEntity(): EntityStorageType =
    when (this) {
      ModelStorageType.AWS_SECRETS_MANAGER -> EntityStorageType.aws_secrets_manager
      ModelStorageType.GOOGLE_SECRET_MANAGER -> EntityStorageType.google_secret_manager
      ModelStorageType.VAULT -> EntityStorageType.vault
      ModelStorageType.AZURE_KEY_VAULT -> EntityStorageType.azure_key_vault
      ModelStorageType.LOCAL_TESTING -> EntityStorageType.local_testing
    }

  fun EntityScopeType.toConfigModel(): ModelScopeType =
    when (this) {
      EntityScopeType.organization -> ModelScopeType.ORGANIZATION
      EntityScopeType.workspace -> ModelScopeType.WORKSPACE
    }

  fun ModelScopeType.toEntity(): EntityScopeType =
    when (this) {
      ModelScopeType.ORGANIZATION -> EntityScopeType.organization
      ModelScopeType.WORKSPACE -> EntityScopeType.workspace
    }

  fun SecretStorageCreate.toEntity(): EntitySecretStorage =
    EntitySecretStorage(
      id = this.id?.value,
      scopeType = this.scopeType.toEntity(),
      scopeId = this.scopeId,
      descriptor = this.descriptor,
      storageType = this.storageType.toEntity(),
      configuredFromEnvironment = this.configuredFromEnvironment,
      tombstone = false,
      createdBy = this.createdBy.value,
      updatedBy = this.createdBy.value,
    )
}
