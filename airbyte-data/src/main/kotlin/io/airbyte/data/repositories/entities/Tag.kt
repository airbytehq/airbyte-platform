package io.airbyte.data.repositories.entities

import io.micronaut.data.annotation.AutoPopulated
import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import java.util.UUID

@MappedEntity("tag")
data class Tag(
  @field:Id
  @AutoPopulated
  var id: UUID? = null,
  var workspaceId: UUID,
  var name: String,
  var color: String,
  @DateCreated
  var createdAt: java.time.OffsetDateTime? = null,
  @DateUpdated
  var updatedAt: java.time.OffsetDateTime? = null,
)