/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import io.airbyte.config.secrets.hydration.SecretsHydrator
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton

private const val SECRET_KEY = "_secret"

/**
 * This class is responsible for fetching both connectors and their secrets (from separate secrets
 * stores). All methods in this class return secrets! Use it carefully.
 */
@Singleton
@Requires(bean = SecretsHydrator::class)
open class SecretsRepositoryReader(
  private val secretsHydrator: SecretsHydrator,
) {
  /**
   * Given a secret coordinate, fetch the secret.
   *
   * @param secretCoordinate secret coordinate
   * @return JsonNode representing the fetched secret
   */
  fun fetchSecretFromDefaultSecretPersistence(secretCoordinate: SecretCoordinate): JsonNode {
    val node = JsonNodeFactory.instance.objectNode()
    node.put(SECRET_KEY, secretCoordinate.fullCoordinate)
    return secretsHydrator.hydrateSecretCoordinateFromDefaultSecretPersistence(node)
  }

  /**
   * Given a secret coordinate, fetch the secret.
   *
   * @param secretCoordinate secret coordinate
   * @return JsonNode representing the fetched secret
   */
  fun fetchSecretFromRuntimeSecretPersistence(
    secretCoordinate: SecretCoordinate,
    runtimeSecretPersistence: RuntimeSecretPersistence,
  ): JsonNode {
    val node = JsonNodeFactory.instance.objectNode()
    node.put(SECRET_KEY, secretCoordinate.fullCoordinate)
    return secretsHydrator.hydrateSecretCoordinateFromRuntimeSecretPersistence(node, runtimeSecretPersistence)
  }

  /**
   * Given a config with _secrets in it, hydrate that config and return the hydrated version.
   *
   * @param configWithSecrets Config with _secrets in it.
   * @return Config with _secrets hydrated.
   */
  fun hydrateConfigFromDefaultSecretPersistence(configWithSecrets: JsonNode?): JsonNode? {
    return if (configWithSecrets != null) {
      secretsHydrator.hydrateFromDefaultSecretPersistence(configWithSecrets)
    } else {
      null
    }
  }

  fun hydrateConfigFromRuntimeSecretPersistence(
    configuration: JsonNode?,
    runtimeSecretPersistence: RuntimeSecretPersistence,
  ): JsonNode? {
    return if (configuration != null) {
      secretsHydrator.hydrateFromRuntimeSecretPersistence(configuration, runtimeSecretPersistence)
    } else {
      null
    }
  }
}
