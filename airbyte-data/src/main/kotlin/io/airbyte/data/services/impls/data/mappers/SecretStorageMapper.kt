/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data.mappers

import io.airbyte.config.SecretStorage as ModelSecretStorage
import io.airbyte.config.SecretStorageScopeType as ModelScopeType
import io.airbyte.config.SecretStorageType as ModelStorageType
import io.airbyte.data.repositories.entities.SecretStorage as EntitySecretStorage
import io.airbyte.db.instance.configs.jooq.generated.enums.SecretStorageScopeType as EntityScopeType
import io.airbyte.db.instance.configs.jooq.generated.enums.SecretStorageType as EntityStorageType

fun EntitySecretStorage.toConfigModel(): ModelSecretStorage =
  ModelSecretStorage(
    id = this.id,
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

fun EntityStorageType.toConfigModel(): ModelStorageType =
  when (this) {
    EntityStorageType.aws_secrets_manager -> ModelStorageType.AWS_SECRETS_MANAGER
    EntityStorageType.google_secret_manager -> ModelStorageType.GOOGLE_SECRET_MANAGER
    EntityStorageType.vault -> ModelStorageType.VAULT
    EntityStorageType.azure_key_vault -> ModelStorageType.AZURE_KEY_VAULT
    EntityStorageType.local_testing -> ModelStorageType.LOCAL_TESTING
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
