/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.SecretStorage
import io.airbyte.config.SecretStorageScopeType
import java.util.UUID

interface SecretStorageService {
  fun findById(id: UUID): SecretStorage?

  fun listByScopeTypeAndScopeId(
    scopeType: SecretStorageScopeType,
    scopeId: UUID,
  ): List<SecretStorage>
}
