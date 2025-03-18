/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.domain.models.SecretConfig
import io.airbyte.domain.models.SecretConfigId

interface SecretConfigService {
  fun findById(id: SecretConfigId): SecretConfig?
}
