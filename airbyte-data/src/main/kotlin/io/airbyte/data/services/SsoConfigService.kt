/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.domain.models.SsoConfig

interface SsoConfigService {
  fun createSsoConfig(config: SsoConfig)
}
