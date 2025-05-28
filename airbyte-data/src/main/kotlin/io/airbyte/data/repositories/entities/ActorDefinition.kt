/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType
import io.airbyte.db.instance.configs.jooq.generated.enums.SourceType
import io.micronaut.data.annotation.AutoPopulated
import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.MappedEntity
import io.micronaut.data.annotation.TypeDef
import io.micronaut.data.model.DataType
import jakarta.persistence.Id
import java.util.UUID

@MappedEntity("actor_definition")
open class ActorDefinition(
  @field:Id
  @AutoPopulated
  var id: UUID? = null,
  var name: String? = null,
  var icon: String? = null,
  @field:TypeDef(type = DataType.OBJECT)
  var actorType: ActorType,
  @field:TypeDef(type = DataType.OBJECT)
  var sourceType: SourceType? = null,
  @DateCreated
  var createdAt: java.time.OffsetDateTime? = null,
  @DateUpdated
  var updatedAt: java.time.OffsetDateTime? = null,
  var tombstone: Boolean = false,
  @field:TypeDef(type = DataType.JSON)
  var resourceRequirements: JsonNode? = null,
  var public: Boolean = false,
  var custom: Boolean = false,
  var maxSecondsBetweenMessages: Int? = null,
  var defaultVersionId: UUID? = null,
  var iconUrl: String? = null,
  @field:TypeDef(type = DataType.JSON)
  var metrics: JsonNode? = null,
  var enterprise: Boolean = false,
)
