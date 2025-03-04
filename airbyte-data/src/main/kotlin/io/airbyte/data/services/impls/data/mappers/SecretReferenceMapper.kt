/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data.mappers

import io.airbyte.config.SecretReference as ModelSecretReference
import io.airbyte.config.SecretReferenceScopeType as ModelScopeType
import io.airbyte.data.repositories.entities.SecretReference as EntitySecretReference
import io.airbyte.db.instance.configs.jooq.generated.enums.SecretReferenceScopeType as EntityScopeType

fun EntitySecretReference.toConfigModel(): ModelSecretReference =
  ModelSecretReference(
    id = this.id!!,
    secretConfigId = this.secretConfigId,
    scopeType = this.scopeType.toConfigModel(),
    scopeId = this.scopeId,
    hydrationPath = this.hydrationPath,
    createdAt = this.createdAt,
    updatedAt = this.updatedAt,
  )

fun EntityScopeType.toConfigModel(): ModelScopeType =
  when (this) {
    EntityScopeType.actor -> ModelScopeType.ACTOR
    EntityScopeType.secret_storage -> ModelScopeType.SECRET_STORAGE
  }

fun ModelScopeType.toEntity(): EntityScopeType =
  when (this) {
    ModelScopeType.ACTOR -> EntityScopeType.actor
    ModelScopeType.SECRET_STORAGE -> EntityScopeType.secret_storage
  }
