/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets.hydration

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.config.secrets.SecretsHelpers
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence
import io.airbyte.config.secrets.persistence.SecretPersistence
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton

/**
 * Adds secrets to a partial config based off a persistence.
 */
@Requires(bean = SecretPersistence::class)
@Singleton
class RealSecretsHydrator(private val secretPersistence: SecretPersistence) : SecretsHydrator {
  override fun hydrateFromDefaultSecretPersistence(partialConfig: JsonNode): JsonNode {
    return SecretsHelpers.combineConfig(partialConfig, secretPersistence)
  }

  override fun hydrateFromRuntimeSecretPersistence(
    partialConfig: JsonNode,
    runtimeSecretPersistence: RuntimeSecretPersistence,
  ): JsonNode {
    return SecretsHelpers.combineConfig(partialConfig, runtimeSecretPersistence)
  }

  override fun hydrateSecretCoordinateFromDefaultSecretPersistence(secretCoordinate: JsonNode): JsonNode {
    return SecretsHelpers.hydrateSecretCoordinate(secretCoordinate, secretPersistence)
  }

  override fun hydrateSecretCoordinateFromRuntimeSecretPersistence(
    secretCoordinate: JsonNode,
    runtimeSecretPersistence: RuntimeSecretPersistence,
  ): JsonNode {
    return SecretsHelpers.hydrateSecretCoordinate(secretCoordinate, runtimeSecretPersistence)
  }
}
