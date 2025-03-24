/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.data.repositories.ConfigTemplateRepository
import io.airbyte.data.repositories.entities.ConfigTemplate
import io.airbyte.data.services.ConfigTemplateService
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
open class ConfigTemplateServiceDataImpl(
  private val repository: ConfigTemplateRepository,
) : ConfigTemplateService {
  override fun getConfigTemplate(configTemplateId: UUID): ConfigTemplate =
    repository.findById(configTemplateId).orElseThrow {
      throw RuntimeException("ConfigTemplate not found")
    }

  override fun listConfigTemplates(organizationId: UUID): List<ConfigTemplate> = repository.findByOrganizationId(organizationId)

  override fun listConfigTemplates(
    organizationId: UUID,
    actorDefinitionId: UUID,
  ): List<ConfigTemplate> = repository.findByOrganizationIdAndActorDefinitionId(organizationId, actorDefinitionId)
}
