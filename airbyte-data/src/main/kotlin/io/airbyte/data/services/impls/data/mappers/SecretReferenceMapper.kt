/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data.mappers

import io.airbyte.domain.models.SecretConfigId
import io.airbyte.domain.models.SecretReferenceCreate
import io.airbyte.domain.models.SecretReferenceId
import io.airbyte.data.repositories.entities.SecretReference as EntitySecretReference
import io.airbyte.db.instance.configs.jooq.generated.enums.SecretReferenceScopeType as EntityScopeType
import io.airbyte.domain.models.SecretReference as ModelSecretReference
import io.airbyte.domain.models.SecretReferenceScopeType as ModelScopeType

object SecretReferenceMapper {
  fun EntitySecretReference.toConfigModel(): ModelSecretReference {
    this.id ?: throw IllegalStateException("Cannot map EntitySecretReference that lacks an id")
    return ModelSecretReference(
      id = SecretReferenceId(id),
      secretConfigId = SecretConfigId(this.secretConfigId),
      scopeType = this.scopeType.toConfigModel(),
      scopeId = this.scopeId,
      hydrationPath = this.hydrationPath,
      createdAt = this.createdAt,
      updatedAt = this.updatedAt,
    )
  }

  fun EntityScopeType.toConfigModel(): ModelScopeType =
    when (this) {
      EntityScopeType.actor -> ModelScopeType.ACTOR
      EntityScopeType.secret_storage -> ModelScopeType.SECRET_STORAGE
      EntityScopeType.connection_template -> ModelScopeType.CONNECTION_TEMPLATE
    }

  fun ModelScopeType.toEntity(): EntityScopeType =
    when (this) {
      ModelScopeType.ACTOR -> EntityScopeType.actor
      ModelScopeType.SECRET_STORAGE -> EntityScopeType.secret_storage
      ModelScopeType.CONNECTION_TEMPLATE -> EntityScopeType.connection_template
    }

  fun SecretReferenceCreate.toEntity(): EntitySecretReference =
    EntitySecretReference(
      id = null,
      secretConfigId = this.secretConfigId.value,
      scopeType = this.scopeType.toEntity(),
      scopeId = this.scopeId,
      hydrationPath = this.hydrationPath,
    )
}
