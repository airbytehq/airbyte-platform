/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets.hydration

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.config.secrets.ConfigWithSecretReferences
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence
import io.airbyte.config.secrets.persistence.SecretPersistence
import java.util.UUID

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
  @Deprecated(
    "Use hydrate instead",
    ReplaceWith("hydrate(partialConfig, defaultSecretPersistence)", "io.airbyte.config.secrets.hydration.SecretsHydrator"),
  )
  fun hydrateFromDefaultSecretPersistence(partialConfig: JsonNode): JsonNode

  /**
   * Takes in the secret coordinate in form of a JSON and fetches the secret from the store.
   *
   * @param secretCoordinate The co-ordinate of the secret in the store in JSON format
   * @return original secret value
   */
  fun hydrateSecretCoordinateFromDefaultSecretPersistence(secretCoordinate: JsonNode): JsonNode

  fun hydrateSecretCoordinate(
    secretCoordinate: JsonNode,
    secretPersistence: SecretPersistence,
  ): JsonNode

  @Deprecated(
    "Use hydrate instead",
    ReplaceWith("hydrate(partialConfig, runtimeSecretPersistence)", "io.airbyte.config.secrets.hydration.SecretsHydrator"),
  )
  fun hydrateFromRuntimeSecretPersistence(
    partialConfig: JsonNode,
    runtimeSecretPersistence: RuntimeSecretPersistence,
  ): JsonNode

  fun hydrateSecretCoordinateFromRuntimeSecretPersistence(
    secretCoordinate: JsonNode,
    runtimeSecretPersistence: RuntimeSecretPersistence,
  ): JsonNode

  /**
   * Adds secrets to a partial config based off a given persistence.
   */
  fun hydrate(
    config: ConfigWithSecretReferences,
    secretPersistence: SecretPersistence,
  ): JsonNode

  /**
   * Adds secrets to a partial config based off a map of persistences.
   */
  fun hydrate(
    config: ConfigWithSecretReferences,
    secretPersistence: Map<UUID?, SecretPersistence>,
  ): JsonNode
}
