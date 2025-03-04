/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.config.SecretStorage
import io.airbyte.config.SecretStorageScopeType
import io.airbyte.data.repositories.SecretStorageRepository
import io.airbyte.data.services.SecretStorageService
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import io.airbyte.data.services.impls.data.mappers.toEntity
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class SecretStorageServiceDataImpl(
  private val secretStorageRepository: SecretStorageRepository,
) : SecretStorageService {
  override fun findById(id: UUID): SecretStorage? = secretStorageRepository.findById(id).orElse(null)?.toConfigModel()

  override fun listByScopeTypeAndScopeId(
    scopeType: SecretStorageScopeType,
    scopeId: UUID,
  ): List<SecretStorage> = secretStorageRepository.listByScopeTypeAndScopeId(scopeType.toEntity(), scopeId).map { it.toConfigModel() }
}
