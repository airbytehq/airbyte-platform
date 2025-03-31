/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets

import com.fasterxml.jackson.databind.JsonNode

data class ConfigWithSecretReferences(
  val config: JsonNode,
  val referencedSecrets: Map<String, SecretReferenceConfig>,
)
