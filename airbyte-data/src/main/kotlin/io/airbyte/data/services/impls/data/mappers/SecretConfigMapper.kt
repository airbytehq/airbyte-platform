/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data.mappers

import io.airbyte.config.SecretConfig as ModelSecretConfig
import io.airbyte.data.repositories.entities.SecretConfig as EntitySecretConfig

fun EntitySecretConfig.toConfigModel(): ModelSecretConfig =
  ModelSecretConfig(
    id = this.id,
    secretStorageId = this.secretStorageId,
    descriptor = this.descriptor,
    externalCoordinate = this.externalCoordinate,
    tombstone = this.tombstone,
    createdBy = this.createdBy,
    updatedBy = this.updatedBy,
    createdAt = this.createdAt,
    updatedAt = this.updatedAt,
  )
