package io.airbyte.data.repositories.entities

import io.micronaut.data.annotation.AutoPopulated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import io.micronaut.data.annotation.TypeDef
import io.micronaut.data.model.DataType
import java.time.OffsetDateTime
import java.util.UUID

@MappedEntity("connection_timeline_event")
data class ConnectionTimelineEvent(
  @field:Id
  @AutoPopulated
  var id: UUID? = null,
  var connectionId: UUID,
  var userId: UUID? = null,
  var eventType: String,
  @field:TypeDef(type = DataType.JSON)
  var summary: String? = null,
  var createdAt: OffsetDateTime? = null,
)
