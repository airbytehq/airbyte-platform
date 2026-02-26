/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.domain.models.SecretConfig
import io.airbyte.domain.models.SecretConfigCreate
import io.airbyte.domain.models.SecretConfigId
import io.airbyte.domain.models.SecretStorageId
import java.time.OffsetDateTime
import java.util.UUID

interface SecretConfigService {
  fun create(secretConfigCreate: SecretConfigCreate): SecretConfig

  fun findById(id: SecretConfigId): SecretConfig?

  fun findByStorageIdAndExternalCoordinate(
    storageId: SecretStorageId,
    coordinate: String,
  ): SecretConfig?

  fun findAirbyteManagedConfigsWithoutReferences(
    excludeCreatedAfter: OffsetDateTime,
    limit: Int,
  ): List<SecretConfig>

  fun findDistinctOrphanedStorageIds(excludeCreatedBefore: OffsetDateTime): List<UUID>

  fun findAirbyteManagedConfigsWithoutReferencesByStorageIds(
    excludeCreatedBefore: OffsetDateTime,
    limit: Int,
    storageIds: List<UUID>,
  ): List<SecretConfig>

  fun deleteByIds(ids: List<SecretConfigId>)
}
