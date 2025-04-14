/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.secrets

import io.airbyte.api.problems.model.generated.ProblemResourceData
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.domain.models.SecretConfig
import io.airbyte.domain.models.SecretConfigId
import jakarta.inject.Singleton
import io.airbyte.data.services.SecretConfigService as SecretConfigRepository

/**
 * Domain service for performing operations related to Airbyte's SecretConfig domain model.
 */
@Singleton
class SecretConfigService(
  private val secretConfigRepository: SecretConfigRepository,
) {
  /**
   * Get the secret config for a given ID.
   *
   * @param id the ID of the secret config to get
   * @return the secret config for the given ID, or null if none exists
   */
  fun getById(id: SecretConfigId): SecretConfig =
    secretConfigRepository.findById(id)
      ?: throw ResourceNotFoundProblem(ProblemResourceData().resourceType(SecretConfig::class.simpleName).resourceId(id.value.toString()))
}
