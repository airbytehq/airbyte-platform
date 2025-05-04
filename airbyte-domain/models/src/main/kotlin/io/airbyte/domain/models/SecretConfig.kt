/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models

import java.time.OffsetDateTime
import java.util.UUID

/**
 * A secret config is a representation of a secret whose value is stored in an external secret storage.
 * The external coordinate describes the location of the secret within the secret storage.
 */
data class SecretConfig(
  val id: SecretConfigId,
  val secretStorageId: UUID,
  val descriptor: String,
  val externalCoordinate: String,
  val tombstone: Boolean = false,
  val airbyteManaged: Boolean,
  val createdBy: UUID?,
  val updatedBy: UUID?,
  val createdAt: OffsetDateTime?,
  val updatedAt: OffsetDateTime?,
)

data class SecretConfigCreate(
  val secretStorageId: SecretStorageId,
  val descriptor: String,
  val externalCoordinate: String,
  val airbyteManaged: Boolean,
  val createdBy: UserId?,
)
