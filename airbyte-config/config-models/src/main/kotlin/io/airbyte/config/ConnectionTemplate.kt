/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.config.JobSyncConfig.NamespaceDefinitionType
import io.airbyte.config.StandardSync.NonBreakingChangesPreference
import io.airbyte.domain.models.OrganizationId
import java.util.UUID

data class ConnectionTemplate(
  val id: UUID,
  val organizationId: OrganizationId,
  val destinationName: String,
  val destinationActorDefinitionId: UUID,
  val destinationConfiguration: JsonNode,
  val namespaceDefinitionType: NamespaceDefinitionType,
  val namespaceFormat: String?,
  val prefix: String?,
  val scheduleType: StandardSync.ScheduleType,
  val scheduleData: ScheduleData?,
  val resourceRequirements: ResourceRequirements?,
  val nonBreakingChangesPreference: NonBreakingChangesPreference,
  val syncOnCreate: Boolean,
)
