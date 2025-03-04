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
  var id: UUID? = null,
  var secretStorageId: UUID,
  var descriptor: String,
  var externalCoordinate: String,
  var tombstone: Boolean = false,
  var createdBy: UUID,
  var updatedBy: UUID,
  @DateCreated
  var createdAt: java.time.OffsetDateTime? = null,
  @DateUpdated
  var updatedAt: java.time.OffsetDateTime? = null,
)
