/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.domain.models.SecretStorage
import io.airbyte.domain.models.SecretStorageId
import io.airbyte.domain.models.SecretStorageScopeType
import java.util.UUID

interface SecretStorageService {
  fun findById(id: SecretStorageId): SecretStorage?

  fun listByScopeTypeAndScopeId(
    scopeType: SecretStorageScopeType,
    scopeId: UUID,
  ): List<SecretStorage>
}
