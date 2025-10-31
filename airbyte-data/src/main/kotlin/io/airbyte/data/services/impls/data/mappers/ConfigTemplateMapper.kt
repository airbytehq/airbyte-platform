/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data.mappers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.data.repositories.entities.ConfigTemplate
import io.airbyte.protocol.models.v0.ConnectorSpecification

typealias EntityConfigTemplate = ConfigTemplate
typealias ModelConfigTemplate = io.airbyte.config.ConfigTemplate

val objectMapper = ObjectMapper()

fun EntityConfigTemplate.toConfigModel(): ModelConfigTemplate {
  val connectorSpec =
    this.userConfigSpec.let {
      objectMapper.readValue(it.toString(), ConnectorSpecification::class.java)
    }

  return ModelConfigTemplate(
    id = this.id!!,
    organizationId = this.organizationId,
    actorDefinitionId = this.actorDefinitionId,
    partialDefaultConfig = this.partialDefaultConfig,
    userConfigSpec = connectorSpec,
    createdAt = this.createdAt,
    updatedAt = this.updatedAt,
  )
}

fun ModelConfigTemplate.toEntity(): EntityConfigTemplate {
  val jsonNodeSpec =
    this.userConfigSpec.let {
      objectMapper.valueToTree<JsonNode>(it)
    }

  return EntityConfigTemplate(
    id = this.id,
    organizationId = this.organizationId,
    actorDefinitionId = this.actorDefinitionId,
    partialDefaultConfig = this.partialDefaultConfig,
    userConfigSpec = jsonNodeSpec,
    createdAt = this.createdAt,
    updatedAt = this.updatedAt,
  )
}
