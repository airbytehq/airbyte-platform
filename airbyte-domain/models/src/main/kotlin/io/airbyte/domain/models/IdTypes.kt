/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models

import java.util.UUID

/**
 * This file contains type-safe wrappers around UUIDs for various entities in the system.
 * These are used to prevent bugs where the wrong UUID is passed to a function.
 */

@JvmInline
value class ActorDefinitionId(
  val value: UUID,
)

@JvmInline
value class ConnectionId(
  val value: UUID,
)

@JvmInline
value class OrganizationId(
  val value: UUID,
)

@JvmInline
value class WorkspaceId(
  val value: UUID,
)

@JvmInline
value class SecretStorageId(
  val value: UUID,
)

@JvmInline
value class SecretConfigId(
  val value: UUID,
)

@JvmInline
value class SecretReferenceId(
  val value: UUID,
)

@JvmInline
value class UserId(
  val value: UUID,
)

@JvmInline
value class ActorId(
  val value: UUID,
)

@JvmInline
value class DestinationCatalogId(
  val value: UUID,
)
