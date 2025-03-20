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
  val id: UUID? = null,
  @field:TypeDef(type = DataType.OBJECT)
  val scopeType: SecretStorageScopeType,
  val scopeId: UUID,
  val descriptor: String,
  @field:TypeDef(type = DataType.OBJECT)
  val storageType: SecretStorageType,
  val configuredFromEnvironment: Boolean,
  val tombstone: Boolean = false,
  val createdBy: UUID,
  val updatedBy: UUID,
  @DateCreated
  val createdAt: java.time.OffsetDateTime? = null,
  @DateUpdated
  val updatedAt: java.time.OffsetDateTime? = null,
)
