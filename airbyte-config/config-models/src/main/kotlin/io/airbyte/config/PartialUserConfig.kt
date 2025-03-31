/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import com.fasterxml.jackson.databind.JsonNode
import java.util.UUID

/**
 * Entity for manipulating PartialUserConfigs
 *
 * The partialUserConfigProperties field is expected to contain a JSON string
 * representing specific user configuration properties.
 *
 * This will be deprecated in favor of the version with sourceId in https://github.com/airbytehq/airbyte-internal-issues/issues/12247
 */

data class PartialUserConfig(
  val id: UUID,
  val workspaceId: UUID,
  val configTemplateId: UUID,
  /**
   * JSON string containing user-specific configuration properties that will be
   * applied to the template configuration
   */
  val partialUserConfigProperties: JsonNode,
)

/**
 * Entity for manipulating PartialUserConfigs with sourceId
 *
 * The partialUserConfigProperties field is expected to contain a JSON string
 * representing specific user configuration properties.
 *
 */
data class PartialUserConfigWithSourceId(
  val id: UUID,
  val workspaceId: UUID,
  val configTemplateId: UUID,
  /**
   * JSON string containing user-specific configuration properties that will be
   * applied to the template configuration
   */
  val partialUserConfigProperties: JsonNode,
  val sourceId: UUID,
)
