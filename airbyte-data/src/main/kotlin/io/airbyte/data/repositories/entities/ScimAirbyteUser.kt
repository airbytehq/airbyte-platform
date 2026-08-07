/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import java.util.UUID

/** Minimal persistence shape used when SCIM links or pre-provisions a global Airbyte User. */
@MappedEntity(value = "user", escape = true)
data class ScimAirbyteUser(
  @field:Id
  val id: UUID = UUID.randomUUID(),
  val name: String,
  val email: String,
)
