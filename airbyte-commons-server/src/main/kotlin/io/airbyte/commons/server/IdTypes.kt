package io.airbyte.commons.server

import java.util.UUID

/**
 * This file contains type-safe wrappers around UUIDs for various entities in the system.
 * These are used to prevent bugs where the wrong UUID is passed to a function.
 */

@JvmInline
value class ConnectionId(
  val value: UUID,
)

@JvmInline
value class OrganizationId(
  val value: UUID,
)
