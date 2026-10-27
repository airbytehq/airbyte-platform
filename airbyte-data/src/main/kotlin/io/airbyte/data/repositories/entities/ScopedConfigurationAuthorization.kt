/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import io.airbyte.db.instance.configs.jooq.generated.enums.ConfigOriginType
import io.airbyte.db.instance.configs.jooq.generated.enums.ConfigResourceType
import io.airbyte.db.instance.configs.jooq.generated.enums.ConfigScopeType
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import io.micronaut.data.annotation.TypeDef
import io.micronaut.data.model.DataType
import java.util.UUID

/** Ownership columns only; arbitrary configuration values are excluded from the query. */
@MappedEntity("scoped_configuration")
data class ScopedConfigurationAuthorization(
  @field:Id val id: UUID,
  val key: String,
  @field:TypeDef(type = DataType.OBJECT) val scopeType: ConfigScopeType,
  val scopeId: UUID,
  @field:TypeDef(type = DataType.OBJECT) val resourceType: ConfigResourceType?,
  val resourceId: UUID?,
  @field:TypeDef(type = DataType.OBJECT) val originType: ConfigOriginType,
  val origin: String,
)
