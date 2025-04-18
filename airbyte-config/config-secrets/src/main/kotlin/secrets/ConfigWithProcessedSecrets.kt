/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.domain.models.SecretReferenceId
import io.airbyte.domain.models.SecretStorageId

/**
 * Bundles a config [JsonNode] with a map of String path to [ProcessedSecretNode] for each of its
 * populated secret values. Allows type safety when working with secret nodes in a config, rather
 * than relying on raw JsonNode parsing.
 */
data class ConfigWithProcessedSecrets(
  val originalConfig: JsonNode,
  val processedSecrets: Map<String, ProcessedSecretNode>,
)

/**
 * Represents an individual secret node in a config. Allows for type-safe operations on secret nodes
 * in a config, rather than relying on raw JsonNode parsing.
 */
data class ProcessedSecretNode(
  val secretCoordinate: SecretCoordinate? = null,
  val rawValue: String? = null,
  val secretStorageId: SecretStorageId? = null,
  val secretReferenceId: SecretReferenceId? = null,
)
