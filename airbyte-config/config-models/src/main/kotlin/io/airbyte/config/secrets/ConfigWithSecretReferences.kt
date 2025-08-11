/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets

import com.fasterxml.jackson.databind.JsonNode
import java.io.Serializable

data class ConfigWithSecretReferences(
  /**
   * Original configuration before fetching secret references.
   * You should almost never use this directly. To get an inlined representation that considers secret references, see .toInlined()
   */
  val originalConfig: JsonNode,
  /**
   * Map of secret coordinates to slot into the configuration.
   */
  val referencedSecrets: Map<String, SecretReferenceConfig>,
) : Serializable
