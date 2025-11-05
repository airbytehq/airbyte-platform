/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import io.micronaut.data.annotation.event.PrePersist
import java.time.OffsetDateTime
import java.util.UUID

/**
 * Entity representing a user group within an organization.
 * Groups are organization-scoped collections of users that can have permissions assigned to them.
 */
@MappedEntity(value = "group", escape = true)
data class Group(
  @field:Id
  var id: UUID? = null,
  var name: String,
  var description: String? = null,
  var organizationId: UUID,
  @DateCreated
  var createdAt: OffsetDateTime? = null,
  @DateUpdated
  var updatedAt: OffsetDateTime? = null,
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
