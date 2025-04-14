/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import io.airbyte.db.instance.configs.jooq.generated.enums.SecretReferenceScopeType
import io.micronaut.data.annotation.AutoPopulated
import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import io.micronaut.data.annotation.Relation
import io.micronaut.data.annotation.TypeDef
import io.micronaut.data.model.DataType
import java.util.UUID

@MappedEntity("secret_reference")
data class SecretReference(
  @field:Id
  @AutoPopulated
  val id: UUID? = null,
  val secretConfigId: UUID,
  @field:TypeDef(type = DataType.OBJECT)
  val scopeType: SecretReferenceScopeType,
  val scopeId: UUID,
  val hydrationPath: String? = null,
  @DateCreated
  val createdAt: java.time.OffsetDateTime? = null,
  @DateUpdated
  val updatedAt: java.time.OffsetDateTime? = null,
)

@MappedEntity("secret_reference")
data class SecretReferenceWithConfig(
  @field:Id
  @AutoPopulated
  val id: UUID? = null,
  val secretConfigId: UUID,
  @field:TypeDef(type = DataType.OBJECT)
  val scopeType: SecretReferenceScopeType,
  val scopeId: UUID,
  val hydrationPath: String? = null,
  @DateCreated
  val createdAt: java.time.OffsetDateTime? = null,
  @DateUpdated
  val updatedAt: java.time.OffsetDateTime? = null,
  @Relation(value = Relation.Kind.MANY_TO_ONE)
  val secretConfig: SecretConfig,
)
