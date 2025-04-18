/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.domain.models.SecretConfig
import io.airbyte.domain.models.SecretConfigCreate
import io.airbyte.domain.models.SecretConfigId
import io.airbyte.domain.models.SecretStorageId

interface SecretConfigService {
  fun create(secretConfigCreate: SecretConfigCreate): SecretConfig

  fun findById(id: SecretConfigId): SecretConfig?

  fun findByStorageIdAndExternalCoordinate(
    storageId: SecretStorageId,
    coordinate: String,
  ): SecretConfig?
}
