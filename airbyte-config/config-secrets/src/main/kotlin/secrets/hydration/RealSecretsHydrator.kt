/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets.hydration

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.config.secrets.ConfigWithSecretReferences
import io.airbyte.config.secrets.InlinedConfigWithSecretRefs
import io.airbyte.config.secrets.SecretsHelpers
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence
import io.airbyte.config.secrets.persistence.SecretPersistence
import io.airbyte.config.secrets.toConfigWithRefs
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import java.util.UUID

/**
 * Adds secrets to a partial config based off a persistence.
 */
@Requires(bean = SecretPersistence::class)
@Singleton
class RealSecretsHydrator(
  private val defaultSecretPersistence: SecretPersistence,
) : SecretsHydrator {
  override fun hydrateFromDefaultSecretPersistence(partialConfig: JsonNode): JsonNode =
    SecretsHelpers.combineConfig(InlinedConfigWithSecretRefs(partialConfig).toConfigWithRefs(), defaultSecretPersistence)

  override fun hydrateFromRuntimeSecretPersistence(
    partialConfig: JsonNode,
    runtimeSecretPersistence: RuntimeSecretPersistence,
  ): JsonNode = SecretsHelpers.combineConfig(InlinedConfigWithSecretRefs(partialConfig).toConfigWithRefs(), runtimeSecretPersistence)

  override fun hydrateSecretCoordinateFromDefaultSecretPersistence(secretCoordinate: JsonNode): JsonNode =
    SecretsHelpers.hydrateSecretCoordinate(secretCoordinate, defaultSecretPersistence)

  override fun hydrateSecretCoordinate(
    secretCoordinate: JsonNode,
    secretPersistence: SecretPersistence,
  ): JsonNode = SecretsHelpers.hydrateSecretCoordinate(secretCoordinate, secretPersistence)

  override fun hydrateSecretCoordinateFromRuntimeSecretPersistence(
    secretCoordinate: JsonNode,
    runtimeSecretPersistence: RuntimeSecretPersistence,
  ): JsonNode = SecretsHelpers.hydrateSecretCoordinate(secretCoordinate, runtimeSecretPersistence)

  override fun hydrate(
    config: ConfigWithSecretReferences,
    secretPersistence: SecretPersistence,
  ): JsonNode = SecretsHelpers.combineConfig(config, secretPersistence)

  override fun hydrate(
    config: ConfigWithSecretReferences,
    secretPersistenceMap: Map<UUID?, SecretPersistence>,
  ): JsonNode = SecretsHelpers.combineConfig(config, secretPersistenceMap)
}
