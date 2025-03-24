/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import com.fasterxml.jackson.databind.JsonNode
import java.time.OffsetDateTime
import java.util.UUID

/**
 * Entity for manipulating ConfigTemplates
 *
 * The userConfigSpec field is expected to contain a JSON object with the following structure:
 * {
 *   advancedAuth?: AdvancedAuth,
 *   advancedAuthGlobalCredentialsAvailable?: boolean,
 *   connectionSpecification?: { [key: string]: any }, // Dynamic key-value pairs
 *   documentationUrl?: string
 * }
 */
data class ConfigTemplate(
  val id: UUID,
  val organizationId: UUID,
  val actorDefinitionId: UUID,
  val partialDefaultConfig: JsonNode,
  /**
   * JSON string containing configuration specification (as JSON spec) with fields:
   * - advancedAuth (optional)
   * - advancedAuthGlobalCredentialsAvailable (optional)
   * - connectionSpecification (optional) - a dynamic object with arbitrary properties
   * - documentationUrl (optional)
   */
  val userConfigSpec: JsonNode,
  val createdAt: OffsetDateTime?,
  val updatedAt: OffsetDateTime?,
)
