/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data.mappers

import io.airbyte.domain.models.SecretConfigId
import io.airbyte.data.repositories.entities.SecretConfig as EntitySecretConfig
import io.airbyte.domain.models.SecretConfig as ModelSecretConfig

fun EntitySecretConfig.toConfigModel(): ModelSecretConfig =
  ModelSecretConfig(
    id = this.id?.let { SecretConfigId(it) },
    secretStorageId = this.secretStorageId,
    descriptor = this.descriptor,
    externalCoordinate = this.externalCoordinate,
    tombstone = this.tombstone,
    airbyteManaged = this.airbyteManaged,
    createdBy = this.createdBy,
    updatedBy = this.updatedBy,
    createdAt = this.createdAt,
    updatedAt = this.updatedAt,
  )
