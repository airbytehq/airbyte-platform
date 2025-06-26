/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import com.fasterxml.jackson.databind.JsonNode
import java.util.UUID

/**
 * Config model for manipulating PartialUserConfigs
 *
 * The partialUserConfigProperties field is expected to contain a JSON string
 * representing specific user configuration properties.
 *
 */

data class PartialUserConfig(
  val id: UUID,
  val workspaceId: UUID,
  val configTemplateId: UUID,
  val actorId: UUID? = null,
)

/**
 * Config model for manipulating PartialUserConfigs with actor details
 */

data class PartialUserConfigWithActorDetails(
  val partialUserConfig: PartialUserConfig,
  val actorIcon: String,
  val actorName: String,
  val configTemplateId: UUID,
)

/**
 * Config model for manipulating PartialUserConfigs with config template details
 */

data class PartialUserConfigWithConfigTemplateAndActorDetails(
  val partialUserConfig: PartialUserConfig,
  val configTemplate: ConfigTemplate,
  val actorName: String,
  val actorIcon: String,
)

data class PartialUserConfigWithFullDetails(
  val partialUserConfig: PartialUserConfig,
  val configTemplate: ConfigTemplate,
  val connectionConfiguration: JsonNode,
  val actorName: String,
  val actorIcon: String,
)
