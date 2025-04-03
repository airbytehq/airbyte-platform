/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import com.fasterxml.jackson.databind.JsonNode
import io.micronaut.data.annotation.AutoPopulated
import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import io.micronaut.data.annotation.TypeDef
import io.micronaut.data.model.DataType
import java.util.UUID

@MappedEntity("partial_user_config")
data class PartialUserConfig(
  @field:Id
  @AutoPopulated
  var id: UUID? = null,
  var workspaceId: UUID,
  var configTemplateId: UUID,
  @field:TypeDef(type = DataType.JSON)
  var partialUserConfigProperties: JsonNode,
  var tombstone: Boolean = false,
  var sourceId: UUID,
  @DateCreated
  var createdAt: java.time.OffsetDateTime? = null,
  @DateUpdated
  var updatedAt: java.time.OffsetDateTime? = null,
)
