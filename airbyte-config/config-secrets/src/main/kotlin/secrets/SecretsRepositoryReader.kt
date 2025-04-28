/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import io.airbyte.config.secrets.hydration.SecretsHydrator
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence
import io.airbyte.config.secrets.persistence.SecretPersistence
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import java.util.UUID

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
  @Deprecated(
    "Use fetchSecretFromSecretPersistence instead",
    ReplaceWith("fetchSecretFromSecretPersistence(secretCoordinate, secretPersistence)", "io.airbyte.config.secrets.SecretsRepositoryReader"),
  )
  fun fetchSecretFromRuntimeSecretPersistence(
    secretCoordinate: SecretCoordinate,
    runtimeSecretPersistence: RuntimeSecretPersistence,
  ): JsonNode {
    val node = JsonNodeFactory.instance.objectNode()
    node.put(SECRET_KEY, secretCoordinate.fullCoordinate)
    return secretsHydrator.hydrateSecretCoordinateFromRuntimeSecretPersistence(node, runtimeSecretPersistence)
  }

  fun fetchSecretFromSecretPersistence(
    secretCoordinate: SecretCoordinate,
    secretPersistence: SecretPersistence,
  ): JsonNode {
    val node = JsonNodeFactory.instance.objectNode()
    node.put(SECRET_KEY, secretCoordinate.fullCoordinate)
    return secretsHydrator.hydrateSecretCoordinate(node, secretPersistence)
  }

  /**
   * Given a config with _secrets in it, hydrate that config and return the hydrated version.
   *
   * @param configWithSecrets Config with _secrets in it.
   * @return Config with _secrets hydrated.
   */
  @Deprecated(
    "Use hydrateConfig instead",
    ReplaceWith("hydrateConfig(configWithSecrets, secretPersistence)", "io.airbyte.config.secrets.SecretsRepositoryReader"),
  )
  fun hydrateConfigFromDefaultSecretPersistence(configWithSecrets: JsonNode?): JsonNode? =
    if (configWithSecrets != null) {
      secretsHydrator.hydrateFromDefaultSecretPersistence(configWithSecrets)
    } else {
      null
    }

  @Deprecated(
    "Use hydrateConfig instead",
    ReplaceWith("hydrateConfig(configWithSecrets, runtimeSecretPersistence)", "io.airbyte.config.secrets.SecretsRepositoryReader"),
  )
  fun hydrateConfigFromRuntimeSecretPersistence(
    configuration: JsonNode?,
    runtimeSecretPersistence: RuntimeSecretPersistence,
  ): JsonNode? =
    if (configuration != null) {
      secretsHydrator.hydrateFromRuntimeSecretPersistence(configuration, runtimeSecretPersistence)
    } else {
      null
    }

  fun hydrateConfig(
    configuration: ConfigWithSecretReferences?,
    secretPersistence: SecretPersistence,
  ): JsonNode? =
    if (configuration != null) {
      secretsHydrator.hydrate(configuration, secretPersistence)
    } else {
      null
    }

  fun hydrateConfig(
    configuration: ConfigWithSecretReferences?,
    secretPersistenceMap: Map<UUID?, SecretPersistence>,
  ): JsonNode? =
    if (configuration != null) {
      secretsHydrator.hydrate(configuration, secretPersistenceMap)
    } else {
      null
    }
}
