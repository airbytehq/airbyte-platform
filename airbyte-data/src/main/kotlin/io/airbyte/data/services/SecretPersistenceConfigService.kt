/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.ScopeType
import io.airbyte.config.SecretPersistenceConfig
import io.airbyte.config.SecretPersistenceCoordinate
import java.util.Optional
import java.util.UUID

interface SecretPersistenceConfigService {
  fun get(
    scope: ScopeType,
    scopeId: UUID,
  ): SecretPersistenceConfig

  fun createOrUpdate(
    scope: ScopeType,
    scopeId: UUID,
    secretPersistenceType: SecretPersistenceConfig.SecretPersistenceType,
    secretPersistenceConfigCoordinate: String,
  ): Optional<SecretPersistenceCoordinate>
}
