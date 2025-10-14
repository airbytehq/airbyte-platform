/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import io.micronaut.data.annotation.event.PrePersist
import java.time.OffsetDateTime
import java.util.UUID

/**
 * Entity representing a user's membership in a group.
 * This is a join table establishing many-to-many relationships between users and groups.
 */
@MappedEntity("group_member")
data class GroupMember(
  @field:Id
  var id: UUID? = null,
  var groupId: UUID,
  var userId: UUID,
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
