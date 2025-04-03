/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets

import com.fasterxml.jackson.databind.JsonNode

data class ConfigWithSecretReferences(
  // what is persisted in DB, with pointers to references
  val config: JsonNode,
  // references point to secrets ({$.password -> secret_123})
  val referencedSecrets: Map<String, SecretReferenceConfig>,
)
