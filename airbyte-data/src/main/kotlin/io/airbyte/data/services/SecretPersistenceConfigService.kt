/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.ScopeType
import io.airbyte.config.SecretPersistenceConfig
import io.airbyte.config.SecretPersistenceCoordinate
import io.airbyte.data.ConfigNotFoundException
import java.io.IOException
import java.util.Optional
import java.util.UUID

interface SecretPersistenceConfigService {
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun get(
    scope: ScopeType,
    scopeId: UUID,
  ): SecretPersistenceConfig

  @Throws(IOException::class)
  fun createOrUpdate(
    scope: ScopeType,
    scopeId: UUID,
    secretPersistenceType: SecretPersistenceConfig.SecretPersistenceType,
    secretPersistenceConfigCoordinate: String,
  ): Optional<SecretPersistenceCoordinate>
}
