/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data.mappers

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.data.repositories.entities.ConfigTemplate

typealias EntityConfigTemplate = ConfigTemplate
typealias ModelConfigTemplate = io.airbyte.config.ConfigTemplate

val objectMapper = ObjectMapper()

fun EntityConfigTemplate.toConfigModel(): ModelConfigTemplate =
  ModelConfigTemplate(
    id = this.id!!,
    organizationId = this.organizationId,
    actorDefinitionId = this.actorDefinitionId,
    partialDefaultConfig = this.partialDefaultConfig,
    userConfigSpec = this.userConfigSpec,
    createdAt = this.createdAt,
    updatedAt = this.updatedAt,
  )

fun ModelConfigTemplate.toEntity(): EntityConfigTemplate =
  EntityConfigTemplate(
    id = this.id,
    organizationId = this.organizationId,
    actorDefinitionId = this.actorDefinitionId,
    partialDefaultConfig = this.partialDefaultConfig,
    userConfigSpec = this.userConfigSpec,
    createdAt = this.createdAt,
    updatedAt = this.updatedAt,
  )
