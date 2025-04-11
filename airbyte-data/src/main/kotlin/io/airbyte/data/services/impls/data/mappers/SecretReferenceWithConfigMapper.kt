/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data.mappers

import io.airbyte.data.services.impls.data.mappers.SecretConfigMapper.toConfigModel
import io.airbyte.data.services.impls.data.mappers.SecretReferenceMapper.toConfigModel
import io.airbyte.domain.models.SecretConfigId
import io.airbyte.domain.models.SecretReferenceId
import io.airbyte.domain.models.SecretReferenceWithConfig
import io.airbyte.data.repositories.entities.SecretReferenceWithConfig as EntitySecretReferenceWithConfig
import io.airbyte.domain.models.SecretReference as ModelSecretReference

object SecretReferenceWithConfigMapper {
  fun EntitySecretReferenceWithConfig.toConfigModel(): SecretReferenceWithConfig {
    this.id ?: throw IllegalStateException("Cannot map EntitySecretReference that lacks an id")
    return SecretReferenceWithConfig(
      secretReference =
        ModelSecretReference(
          id = SecretReferenceId(id),
          secretConfigId = SecretConfigId(this.secretConfigId),
          scopeType = this.scopeType.toConfigModel(),
          scopeId = this.scopeId,
          hydrationPath = this.hydrationPath,
          createdAt = this.createdAt,
          updatedAt = this.updatedAt,
        ),
      secretConfig = this.secretConfig.toConfigModel(),
    )
  }
}
