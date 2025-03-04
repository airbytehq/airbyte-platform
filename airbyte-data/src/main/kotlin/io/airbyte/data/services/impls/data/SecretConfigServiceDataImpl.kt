/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.config.SecretConfig
import io.airbyte.data.repositories.SecretConfigRepository
import io.airbyte.data.services.SecretConfigService
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class SecretConfigServiceDataImpl(
  private val secretConfigRepository: SecretConfigRepository,
) : SecretConfigService {
  override fun findById(id: UUID): SecretConfig? = secretConfigRepository.findById(id).orElse(null)?.toConfigModel()
}
