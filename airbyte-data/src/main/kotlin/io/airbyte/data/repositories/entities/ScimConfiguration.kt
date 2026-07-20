/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import io.micronaut.data.annotation.event.PrePersist
import java.time.OffsetDateTime
import java.util.UUID

/** Persistence representation of an organization's SCIM configuration. */
@MappedEntity("scim_configuration")
open class ScimConfiguration(
  @field:Id
  var id: UUID? = null,
  var organizationId: UUID,
  var tokenHash: String? = null,
  var idpProvider: String? = null,
  var enabled: Boolean = false,
  @DateCreated
  var createdAt: OffsetDateTime? = null,
  @DateUpdated
  var updatedAt: OffsetDateTime? = null,
  var createdByUserId: UUID? = null,
  var tokenIssuedAt: OffsetDateTime? = null,
  var tokenIssuedByUserId: UUID? = null,
  var disabledAt: OffsetDateTime? = null,
  var disabledByUserId: UUID? = null,
) {
  @PrePersist
  fun prePersist() {
    if (id == null) {
      id = UUID.randomUUID()
    }
  }
}
