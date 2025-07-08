/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import io.micronaut.core.annotation.Introspected
import io.micronaut.data.annotation.AutoPopulated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import io.micronaut.data.annotation.MappedProperty
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

@Introspected
data class ConnectionTimelineEventMinimal(
  val id: UUID,
  @MappedProperty("connection_name")
  val connectionName: String?,
  @MappedProperty("connection_id")
  val connectionId: UUID,
  @MappedProperty("created_at")
  val createdAt: OffsetDateTime?,
  @MappedProperty("event_type")
  val eventType: String,
)
