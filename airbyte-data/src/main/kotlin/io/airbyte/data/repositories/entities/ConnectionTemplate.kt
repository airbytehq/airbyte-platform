/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.db.instance.configs.jooq.generated.enums.NamespaceDefinitionType
import io.airbyte.db.instance.configs.jooq.generated.enums.NonBreakingChangePreferenceType
import io.airbyte.db.instance.configs.jooq.generated.enums.ScheduleType
import io.micronaut.data.annotation.AutoPopulated
import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import io.micronaut.data.annotation.TypeDef
import io.micronaut.data.model.DataType
import java.util.UUID

@MappedEntity("connection_template")
data class ConnectionTemplate(
  @field:Id
  @AutoPopulated
  var id: UUID? = null,
  var organizationId: UUID,
  var destinationName: String,
  var destinationDefinitionId: UUID,
  @field:TypeDef(type = DataType.JSON)
  var destinationConfig: JsonNode,
  @field:TypeDef(type = DataType.OBJECT)
  var namespaceDefinition: NamespaceDefinitionType,
  var namespaceFormat: String?,
  var prefix: String?,
  @field:TypeDef(type = DataType.OBJECT)
  var scheduleType: ScheduleType,
  @field:TypeDef(type = DataType.JSON)
  var scheduleData: JsonNode?,
  @field:TypeDef(type = DataType.JSON)
  var resourceRequirements: JsonNode?,
  @field:TypeDef(type = DataType.OBJECT)
  var nonBreakingChangesPreference: NonBreakingChangePreferenceType,
  var syncOnCreate: Boolean = true,
  var tombstone: Boolean = false,
  @DateCreated
  var createdAt: java.time.OffsetDateTime? = null,
  @DateUpdated
  var updatedAt: java.time.OffsetDateTime? = null,
)
