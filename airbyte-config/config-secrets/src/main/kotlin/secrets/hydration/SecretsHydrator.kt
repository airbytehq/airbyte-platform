/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets.hydration

import com.fasterxml.jackson.databind.JsonNode

/**
 * Adds secrets to a partial config.
 */
interface SecretsHydrator {
  /**
   * Adds secrets to a partial config.
   *
   * @param partialConfig partial config (without secrets)
   * @return full config with secrets
   */
  fun hydrate(partialConfig: JsonNode): JsonNode

  /**
   * Takes in the secret coordinate in form of a JSON and fetches the secret from the store.
   *
   * @param secretCoordinate The co-ordinate of the secret in the store in JSON format
   * @return original secret value
   */
  fun hydrateSecretCoordinate(secretCoordinate: JsonNode): JsonNode
}
