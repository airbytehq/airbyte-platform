/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import io.micronaut.data.annotation.AutoPopulated
import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import java.time.OffsetDateTime
import java.util.UUID

@MappedEntity("dataplane_heartbeat_log")
data class DataplaneHeartbeatLog(
  @field:Id
  @AutoPopulated
  var id: UUID? = null,
  var dataplaneId: UUID,
  var controlPlaneVersion: String,
  var dataplaneVersion: String,
  @DateCreated
  var createdAt: OffsetDateTime? = null,
)
