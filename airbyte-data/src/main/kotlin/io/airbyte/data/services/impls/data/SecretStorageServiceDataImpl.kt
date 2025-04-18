/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.data.repositories.SecretStorageRepository
import io.airbyte.data.services.SecretStorageService
import io.airbyte.data.services.impls.data.mappers.SecretStorageMapper.toConfigModel
import io.airbyte.data.services.impls.data.mappers.SecretStorageMapper.toEntity
import io.airbyte.domain.models.SecretStorage
import io.airbyte.domain.models.SecretStorageId
import io.airbyte.domain.models.SecretStorageScopeType
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class SecretStorageServiceDataImpl(
  private val secretStorageRepository: SecretStorageRepository,
) : SecretStorageService {
  override fun findById(id: SecretStorageId): SecretStorage? = secretStorageRepository.findById(id.value).orElse(null)?.toConfigModel()

  override fun listByScopeTypeAndScopeId(
    scopeType: SecretStorageScopeType,
    scopeId: UUID,
  ): List<SecretStorage> = secretStorageRepository.listByScopeTypeAndScopeId(scopeType.toEntity(), scopeId).map { it.toConfigModel() }
}
