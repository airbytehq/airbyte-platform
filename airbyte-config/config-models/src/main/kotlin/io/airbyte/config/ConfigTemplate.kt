/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.protocol.models.v0.AdvancedAuth
import io.airbyte.protocol.models.v0.ConnectorSpecification
import java.time.OffsetDateTime
import java.util.UUID

data class ConfigTemplate(
  val id: UUID,
  val organizationId: UUID? = null,
  val actorDefinitionId: UUID,
  val partialDefaultConfig: JsonNode,
  /**
   * JSON string containing configuration specification (as JSON spec) with fields:
   * - advancedAuth (optional)
   * - advancedAuthGlobalCredentialsAvailable (optional)
   * - connectionSpecification (optional) - a dynamic object with arbitrary properties
   * - documentationUrl (optional)
   */
  val userConfigSpec: ConnectorSpecification,
  val advancedAuth: AdvancedAuth? = null,
  val advancedAuthGlobalCredentialsAvailable: Boolean? = null,
  val createdAt: OffsetDateTime? = null,
  val updatedAt: OffsetDateTime? = null,
)

data class ConfigTemplateWithActorDetails(
  val configTemplate: ConfigTemplate,
  val actorName: String,
  val actorIcon: String,
)
