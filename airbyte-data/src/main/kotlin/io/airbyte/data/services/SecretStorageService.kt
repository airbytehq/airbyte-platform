/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.domain.models.PatchField
import io.airbyte.domain.models.PatchField.Absent
import io.airbyte.domain.models.SecretStorage
import io.airbyte.domain.models.SecretStorageCreate
import io.airbyte.domain.models.SecretStorageId
import io.airbyte.domain.models.SecretStorageScopeType
import io.airbyte.domain.models.SecretStorageType
import io.airbyte.domain.models.UserId
import java.util.UUID

interface SecretStorageService {
  fun create(secretStorageCreate: SecretStorageCreate): SecretStorage

  /**
   * Patches the secret storage with the ID from the given patch. Any non-null value in the patch
   * overrides the corresponding value in the existing secret storage.
   * Note: This function is not able to update a non-null value to null, because of how the
   *
   */
  fun patch(
    id: SecretStorageId,
    updatedBy: UserId,
    scopeType: PatchField<SecretStorageScopeType> = Absent,
    scopeId: PatchField<UUID> = Absent,
    descriptor: PatchField<String> = Absent,
    storageType: PatchField<SecretStorageType> = Absent,
    configuredFromEnvironment: PatchField<Boolean> = Absent,
    tombstone: PatchField<Boolean> = Absent,
  ): SecretStorage

  fun findById(id: SecretStorageId): SecretStorage?

  fun listByScopeTypeAndScopeId(
    scopeType: SecretStorageScopeType,
    scopeId: UUID,
  ): List<SecretStorage>
}
