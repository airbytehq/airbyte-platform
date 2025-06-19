/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import java.util.UUID

@MappedEntity("dataplane")
data class Dataplane(
  @field:Id
  var id: UUID? = UUID.randomUUID(),
  var dataplaneGroupId: UUID,
  var name: String,
  var enabled: Boolean,
  @DateCreated
  var createdAt: java.time.OffsetDateTime? = null,
  @DateUpdated
  var updatedAt: java.time.OffsetDateTime? = null,
  var tombstone: Boolean,
  var serviceAccountId: UUID? = null,
)
