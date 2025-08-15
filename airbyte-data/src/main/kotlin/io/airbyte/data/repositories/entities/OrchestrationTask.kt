/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import java.util.UUID

@MappedEntity("orchestration_task")
data class OrchestrationTask(
  @field:Id
  var id: UUID = UUID.randomUUID(),
  var orchestrationId: UUID,
  var orchestrationVersionId: UUID,
  var type: String,
  var taskDefinitionId: UUID,
  var dependsOn: List<UUID>,
  @DateCreated
  var createdAt: java.time.OffsetDateTime? = null,
)
