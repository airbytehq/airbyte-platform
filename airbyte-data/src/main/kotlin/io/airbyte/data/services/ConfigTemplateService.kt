/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.config.ConfigTemplateWithActorDetails
import io.airbyte.domain.models.ActorDefinitionId
import io.airbyte.domain.models.OrganizationId
import java.util.UUID

/**
 * A service that manages config templates
 */
interface ConfigTemplateService {
  fun getConfigTemplate(configTemplateId: UUID): ConfigTemplateWithActorDetails

  fun getConfigTemplate(
    configTemplateId: UUID,
    workspaceId: UUID,
  ): ConfigTemplateWithActorDetails

  fun listConfigTemplatesForOrganization(organizationId: OrganizationId): List<ConfigTemplateWithActorDetails>

  fun createTemplate(
    organizationId: OrganizationId,
    actorDefinitionId: ActorDefinitionId,
    partialDefaultConfig: JsonNode,
    userConfigSpec: JsonNode? = null,
  ): ConfigTemplateWithActorDetails

  fun updateTemplate(
    configTemplateId: UUID,
    organizationId: OrganizationId,
    partialDefaultConfig: JsonNode? = null,
    userConfigSpec: JsonNode? = null,
  ): ConfigTemplateWithActorDetails
}
