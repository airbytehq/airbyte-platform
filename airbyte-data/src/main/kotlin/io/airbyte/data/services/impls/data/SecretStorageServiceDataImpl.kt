/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.data.repositories.SecretStorageRepository
import io.airbyte.data.services.SecretStorageService
import io.airbyte.data.services.impls.data.mappers.SecretStorageMapper.toConfigModel
import io.airbyte.data.services.impls.data.mappers.SecretStorageMapper.toEntity
import io.airbyte.domain.models.PatchField
import io.airbyte.domain.models.PatchField.Companion.applyTo
import io.airbyte.domain.models.SecretStorage
import io.airbyte.domain.models.SecretStorageCreate
import io.airbyte.domain.models.SecretStorageId
import io.airbyte.domain.models.SecretStorageScopeType
import io.airbyte.domain.models.SecretStorageType
import io.airbyte.domain.models.UserId
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class SecretStorageServiceDataImpl(
  private val secretStorageRepository: SecretStorageRepository,
) : SecretStorageService {
  override fun create(secretStorageCreate: SecretStorageCreate): SecretStorage =
    secretStorageRepository.save(secretStorageCreate.toEntity()).toConfigModel()

  override fun patch(
    id: SecretStorageId,
    updatedBy: UserId,
    scopeType: PatchField<SecretStorageScopeType>,
    scopeId: PatchField<UUID>,
    descriptor: PatchField<String>,
    storageType: PatchField<SecretStorageType>,
    configuredFromEnvironment: PatchField<Boolean>,
    tombstone: PatchField<Boolean>,
  ): SecretStorage {
    val existing = findById(id) ?: throw IllegalArgumentException("Secret storage with id $id not found")

    val patched =
      existing.copy(
        scopeType = scopeType.applyTo(existing.scopeType),
        scopeId = scopeId.applyTo(existing.scopeId),
        descriptor = descriptor.applyTo(existing.descriptor),
        storageType = storageType.applyTo(existing.storageType),
        configuredFromEnvironment = configuredFromEnvironment.applyTo(existing.configuredFromEnvironment),
        tombstone = tombstone.applyTo(existing.tombstone),
        updatedBy = updatedBy.value,
      )

    return secretStorageRepository.update(patched.toEntity()).toConfigModel()
  }

  override fun findById(id: SecretStorageId): SecretStorage? = secretStorageRepository.findById(id.value).orElse(null)?.toConfigModel()

  override fun listByScopeTypeAndScopeId(
    scopeType: SecretStorageScopeType,
    scopeId: UUID,
  ): List<SecretStorage> = secretStorageRepository.listByScopeTypeAndScopeId(scopeType.toEntity(), scopeId).map { it.toConfigModel() }
}
