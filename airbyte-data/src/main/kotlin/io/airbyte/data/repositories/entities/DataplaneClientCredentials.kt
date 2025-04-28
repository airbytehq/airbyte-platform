/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import java.util.UUID

@MappedEntity("dataplane_client_credentials")
open class DataplaneClientCredentials(
  @field:Id
  var id: UUID,
  var dataplaneId: UUID,
  var clientId: String,
  var clientSecret: String,
  @DateCreated
  var createdAt: java.time.OffsetDateTime? = null,
)
