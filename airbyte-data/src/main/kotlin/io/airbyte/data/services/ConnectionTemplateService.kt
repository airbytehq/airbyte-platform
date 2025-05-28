/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.config.ConnectionTemplate
import io.airbyte.config.JobSyncConfig.NamespaceDefinitionType
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.ScheduleData
import io.airbyte.config.StandardSync.NonBreakingChangesPreference
import io.airbyte.domain.models.ActorDefinitionId
import io.airbyte.domain.models.OrganizationId

interface ConnectionTemplateService {
  fun createTemplate(
    organizationId: OrganizationId,
    destinationName: String,
    actorDefinitionId: ActorDefinitionId,
    destinationConfig: JsonNode,
    namespaceDefinitionType: NamespaceDefinitionType,
    namespaceFormat: String?,
    prefix: String?,
    scheduleData: ScheduleData?,
    resourceRequirements: ResourceRequirements?,
    nonBreakingChangesPreference: NonBreakingChangesPreference,
    syncOnCreate: Boolean,
  ): ConnectionTemplate
}
