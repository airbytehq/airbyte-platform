/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType
import io.micronaut.data.annotation.AutoPopulated
import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.MappedEntity
import io.micronaut.data.annotation.TypeDef
import io.micronaut.data.model.DataType
import jakarta.persistence.Id
import java.util.UUID

@MappedEntity("actor")
open class Actor(
  @field:Id
  @AutoPopulated
  var id: UUID? = null,
  var workspaceId: UUID,
  var actorDefinitionId: UUID,
  var name: String,
  @field:TypeDef(type = DataType.JSON)
  var configuration: JsonNode,
  @field:TypeDef(type = DataType.OBJECT)
  var actorType: ActorType,
  var tombstone: Boolean = false,
  @DateCreated
  var createdAt: java.time.OffsetDateTime? = null,
  @DateUpdated
  var updatedAt: java.time.OffsetDateTime? = null,
  @field:TypeDef(type = DataType.JSON)
  var resourceRequirements: JsonNode? = null,
)
