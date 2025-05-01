/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import io.micronaut.data.annotation.AutoPopulated
import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import java.util.UUID

@MappedEntity("secret_config")
data class SecretConfig(
  @field:Id
  @AutoPopulated
  val id: UUID? = null,
  val secretStorageId: UUID,
  val descriptor: String,
  val externalCoordinate: String,
  val tombstone: Boolean = false,
  val airbyteManaged: Boolean,
  val createdBy: UUID? = null,
  val updatedBy: UUID? = null,
  @DateCreated
  val createdAt: java.time.OffsetDateTime? = null,
  @DateUpdated
  val updatedAt: java.time.OffsetDateTime? = null,
)
