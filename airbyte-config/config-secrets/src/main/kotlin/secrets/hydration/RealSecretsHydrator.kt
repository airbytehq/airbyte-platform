/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
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
class RealSecretsHydrator(
  private val defaultSecretPersistence: SecretPersistence,
) : SecretsHydrator {
  override fun hydrateFromDefaultSecretPersistence(partialConfig: JsonNode): JsonNode =
    SecretsHelpers.combineConfig(partialConfig, defaultSecretPersistence)

  override fun hydrateFromRuntimeSecretPersistence(
    partialConfig: JsonNode,
    runtimeSecretPersistence: RuntimeSecretPersistence,
  ): JsonNode = SecretsHelpers.combineConfig(partialConfig, runtimeSecretPersistence)

  override fun hydrateSecretCoordinateFromDefaultSecretPersistence(secretCoordinate: JsonNode): JsonNode =
    SecretsHelpers.hydrateSecretCoordinate(secretCoordinate, defaultSecretPersistence)

  override fun hydrateSecretCoordinateFromRuntimeSecretPersistence(
    secretCoordinate: JsonNode,
    runtimeSecretPersistence: RuntimeSecretPersistence,
  ): JsonNode = SecretsHelpers.hydrateSecretCoordinate(secretCoordinate, runtimeSecretPersistence)

  override fun hydrate(
    config: JsonNode,
    secretPersistence: SecretPersistence,
  ): JsonNode = SecretsHelpers.combineConfig(config, secretPersistence)
}
