/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.SecretConfig
import java.util.UUID

interface SecretConfigService {
  fun findById(id: UUID): SecretConfig?
}
