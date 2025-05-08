/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.repositories.domain

import com.fasterxml.jackson.databind.JsonNode
import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import io.micronaut.data.annotation.TypeDef
import io.micronaut.data.model.DataType
import java.time.OffsetDateTime
import java.util.UUID

@MappedEntity("commands")
data class Command(
  @field:Id
  val id: String,
  val workloadId: String,
  val commandType: String,
  @TypeDef(type = DataType.JSON)
  val commandInput: JsonNode,
  val workspaceId: UUID,
  val organizationId: UUID,
  @field:DateCreated
  val createdAt: OffsetDateTime?,
  @field:DateUpdated
  val updatedAt: OffsetDateTime?,
)
