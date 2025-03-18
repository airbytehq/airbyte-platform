/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models

import java.util.UUID

/**
 * This file contains type-safe wrappers around UUIDs for various entities in the system.
 * These are used to prevent bugs where the wrong UUID is passed to a function.
 */

interface IdType {
  val value: UUID
}

@JvmInline
value class ConnectionId(
  override val value: UUID,
) : IdType

@JvmInline
value class OrganizationId(
  override val value: UUID,
) : IdType

@JvmInline
value class WorkspaceId(
  override val value: UUID,
) : IdType

@JvmInline
value class SecretStorageId(
  override val value: UUID,
) : IdType

@JvmInline
value class SecretConfigId(
  override val value: UUID,
) : IdType

@JvmInline
value class SecretReferenceId(
  override val value: UUID,
) : IdType
