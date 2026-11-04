/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import com.fasterxml.jackson.databind.JsonNode
import io.micronaut.data.annotation.AutoPopulated
import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import io.micronaut.data.annotation.TypeDef
import io.micronaut.data.model.DataType
import java.time.OffsetDateTime
import java.util.UUID

@MappedEntity("dataplane_diagnostic")
data class DataplaneDiagnostic(
  @field:Id
  @AutoPopulated
  var id: UUID? = null,
  var dataplaneId: UUID,
  var organizationId: UUID,
  var diagnosticKind: String,
  var targetId: UUID,
  var status: String,
  var checkedAt: OffsetDateTime,
  var errorCode: String? = null,
  var errorMessage: String? = null,
  @field:TypeDef(type = DataType.JSON)
  var observation: JsonNode? = null,
  @DateCreated
  var createdAt: OffsetDateTime? = null,
  @DateUpdated
  var updatedAt: OffsetDateTime? = null,
)
