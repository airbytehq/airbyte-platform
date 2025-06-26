/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data.mappers

import io.airbyte.domain.models.SecretConfigCreate
import io.airbyte.domain.models.SecretConfigId
import io.airbyte.data.repositories.entities.SecretConfig as EntitySecretConfig
import io.airbyte.domain.models.SecretConfig as ModelSecretConfig

object SecretConfigMapper {
  fun EntitySecretConfig.toConfigModel(): ModelSecretConfig {
    this.id ?: throw IllegalStateException("Cannot map EntitySecretConfig that lacks an id")
    return ModelSecretConfig(
      id = SecretConfigId(id),
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
  }

  fun SecretConfigCreate.toEntity(): EntitySecretConfig =
    EntitySecretConfig(
      id = null,
      secretStorageId = this.secretStorageId.value,
      descriptor = this.descriptor,
      externalCoordinate = this.externalCoordinate,
      tombstone = false,
      airbyteManaged = this.airbyteManaged,
      createdBy = this.createdBy?.value,
      updatedBy = this.createdBy?.value,
    )
}
