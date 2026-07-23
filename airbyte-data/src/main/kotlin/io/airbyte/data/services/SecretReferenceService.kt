/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.domain.models.SecretConfigId
import io.airbyte.domain.models.SecretReference
import io.airbyte.domain.models.SecretReferenceCreate
import io.airbyte.domain.models.SecretReferenceId
import io.airbyte.domain.models.SecretReferenceScopeType
import io.airbyte.domain.models.SecretReferenceWithConfig
import java.util.UUID

interface SecretReferenceService {
  fun createAndReplace(secretReference: SecretReferenceCreate): SecretReference

  fun deleteByScopeTypeAndScopeId(
    scopeType: SecretReferenceScopeType,
    scopeId: UUID,
  )

  fun deleteByScopeTypeAndScopeIdAndHydrationPath(
    scopeType: SecretReferenceScopeType,
    scopeId: UUID,
    hydrationPath: String,
  )

  fun findById(id: SecretReferenceId): SecretReference?

  fun listByScopeTypeAndScopeId(
    scopeType: SecretReferenceScopeType,
    scopeId: UUID,
  ): List<SecretReference>

  /**
   * Returns true if any secret reference still points at the given secret config. Used to determine
   * whether an Airbyte-managed secret config has become orphaned and is safe to delete.
   */
  fun existsBySecretConfigId(secretConfigId: SecretConfigId): Boolean

  fun listWithConfigByScopeTypeAndScopeId(
    scopeType: SecretReferenceScopeType,
    scopeId: UUID,
  ): List<SecretReferenceWithConfig>

  fun listWithConfigByScopeTypeAndScopeIds(
    scopeType: SecretReferenceScopeType,
    scopeIds: List<UUID>,
  ): List<SecretReferenceWithConfig>
}
