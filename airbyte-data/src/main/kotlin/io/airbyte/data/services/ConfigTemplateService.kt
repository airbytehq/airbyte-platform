/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.data.repositories.entities.ConfigTemplate
import java.util.UUID

/**
 * A service that manages config templates
 */
interface ConfigTemplateService {
  fun getConfigTemplate(configTemplateId: UUID): ConfigTemplate

  fun listConfigTemplates(organizationId: UUID): List<ConfigTemplate>

  fun listConfigTemplates(
    organizationId: UUID,
    actorDefinitionId: UUID,
  ): List<ConfigTemplate>
}
