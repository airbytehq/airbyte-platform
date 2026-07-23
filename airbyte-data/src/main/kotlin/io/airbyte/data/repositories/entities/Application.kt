/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import java.util.UUID

@MappedEntity("application")
open class Application(
  @field:Id
  var id: UUID? = null,
  var authUserId: String? = null,
  var name: String,
  var clientId: String,
  var clientSecret: String,
  @DateCreated
  var createdAt: java.time.OffsetDateTime? = null,
)
