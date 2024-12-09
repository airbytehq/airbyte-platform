package io.airbyte.data.repositories.entities

import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import io.micronaut.data.annotation.event.PrePersist
import java.util.UUID

@MappedEntity("organization")
open class Organization(
  @field:Id
  var id: UUID? = null,
  var name: String,
  var userId: UUID? = null,
  var email: String,
  var tombstone: Boolean = false,
  @DateCreated
  var createdAt: java.time.OffsetDateTime? = null,
  @DateUpdated
  var updatedAt: java.time.OffsetDateTime? = null,
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
