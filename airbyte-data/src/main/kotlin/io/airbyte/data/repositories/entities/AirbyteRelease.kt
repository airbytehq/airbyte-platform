/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import io.micronaut.data.annotation.event.PrePersist
import java.time.OffsetDateTime
import java.util.UUID

@MappedEntity("airbyte_release")
data class AirbyteRelease(
  @field:Id
  var id: UUID? = null,
  var version: String,
  var sourceCommit: String,
  var publishedAt: OffsetDateTime,
  @DateCreated
  var createdAt: OffsetDateTime? = null,
) {
  // Use @PrePersist instead of @AutoPopulated so that we can set the id field
  // if desired prior to insertion.
  @PrePersist
  fun prePersist() {
    if (id == null) {
      id = UUID.randomUUID()
    }
  }
}
