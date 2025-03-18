/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.data.repositories.SecretConfigRepository
import io.airbyte.data.services.SecretConfigService
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import io.airbyte.domain.models.SecretConfig
import io.airbyte.domain.models.SecretConfigId
import jakarta.inject.Singleton

@Singleton
class SecretConfigServiceDataImpl(
  private val secretConfigRepository: SecretConfigRepository,
) : SecretConfigService {
  override fun findById(id: SecretConfigId): SecretConfig? = secretConfigRepository.findById(id.value).orElse(null)?.toConfigModel()
}
