/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.data.repositories.SecretConfigRepository
import io.airbyte.data.services.SecretConfigService
import io.airbyte.data.services.impls.data.mappers.SecretConfigMapper.toConfigModel
import io.airbyte.data.services.impls.data.mappers.SecretConfigMapper.toEntity
import io.airbyte.domain.models.SecretConfig
import io.airbyte.domain.models.SecretConfigCreate
import io.airbyte.domain.models.SecretConfigId
import io.airbyte.domain.models.SecretStorageId
import jakarta.inject.Singleton

@Singleton
class SecretConfigServiceDataImpl(
  private val secretConfigRepository: SecretConfigRepository,
) : SecretConfigService {
  override fun create(secretConfigCreate: SecretConfigCreate): SecretConfig =
    secretConfigRepository.save(secretConfigCreate.toEntity()).toConfigModel()

  override fun findById(id: SecretConfigId): SecretConfig? = secretConfigRepository.findById(id.value).orElse(null)?.toConfigModel()

  override fun findByStorageIdAndExternalCoordinate(
    storageId: SecretStorageId,
    coordinate: String,
  ): SecretConfig? = secretConfigRepository.findBySecretStorageIdAndExternalCoordinate(storageId.value, coordinate)?.toConfigModel()
}
