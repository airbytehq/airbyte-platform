/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.SecretReference
import io.airbyte.config.SecretReferenceScopeType
import java.util.UUID

interface SecretReferenceService {
  fun findById(id: UUID): SecretReference?

  fun listByScopeTypeAndScopeId(
    scopeType: SecretReferenceScopeType,
    scopeId: UUID,
  ): List<SecretReference>
}
