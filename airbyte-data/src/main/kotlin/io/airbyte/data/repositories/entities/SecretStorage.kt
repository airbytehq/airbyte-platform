/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import io.airbyte.db.instance.configs.jooq.generated.enums.SecretStorageScopeType
import io.airbyte.db.instance.configs.jooq.generated.enums.SecretStorageType
import io.micronaut.data.annotation.AutoPopulated
import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import io.micronaut.data.annotation.TypeDef
import io.micronaut.data.model.DataType
import java.util.UUID

@MappedEntity("secret_storage")
data class SecretStorage(
  @field:Id
  @AutoPopulated
  var id: UUID? = null,
  @field:TypeDef(type = DataType.OBJECT)
  var scopeType: SecretStorageScopeType,
  var scopeId: UUID,
  var descriptor: String,
  @field:TypeDef(type = DataType.OBJECT)
  var storageType: SecretStorageType,
  var configuredFromEnvironment: Boolean,
  var tombstone: Boolean = false,
  var createdBy: UUID,
  var updatedBy: UUID,
  @DateCreated
  var createdAt: java.time.OffsetDateTime? = null,
  @DateUpdated
  var updatedAt: java.time.OffsetDateTime? = null,
)
