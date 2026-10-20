/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import java.util.UUID

/** Ownership columns only; command input is never read for authorization. */
@MappedEntity("commands")
data class AuthorizationCommand(
  @field:Id val id: String,
  val workspaceId: UUID,
  val organizationId: UUID,
)
