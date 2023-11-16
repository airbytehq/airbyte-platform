/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence
import io.airbyte.config.secrets.persistence.SecretPersistence
import io.airbyte.protocol.models.ConnectorSpecification
import io.airbyte.validation.json.JsonSchemaValidator
import io.airbyte.validation.json.JsonValidationException
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

private val logger = KotlinLogging.logger {}

/**
 * This class takes secrets as arguments but never returns a secrets as return values (even the ones
 * that are passed in as arguments). It is responsible for writing connector secrets to the correct
 * secrets store and then making sure the remainder of the configuration is written to the Config
 * Database.
 */
@Singleton
@Requires(bean = SecretPersistence::class)
open class SecretsRepositoryWriter(
  private val secretPersistence: SecretPersistence,
) {
  val validator: JsonSchemaValidator = JsonSchemaValidator()

  /**
   * Detects secrets in the configuration. Writes them to the secrets store. It returns the config
   * stripped of secrets (replaced with pointers to the secrets store).
   *
   * Uses the environment secret persistence to store secrets.
   *
   * @param workspaceId workspace id for the config
   * @param fullConfig full config
   * @param spec connector specification
   * @return partial config
   */
  fun statefulSplitSecretsToDefaultSecretPersistence(
    workspaceId: UUID,
    fullConfig: JsonNode,
    spec: ConnectorSpecification,
  ): JsonNode {
    return splitSecretConfig(workspaceId, fullConfig, spec, secretPersistence)
  }

  /**
   * Detects secrets in the configuration. Writes them to the secrets store. It returns the config
   * stripped of secrets (replaced with pointers to the secrets store).
   *
   * Uses the runtime secret persistence to store secrets.
   *
   * @param workspaceId workspace id for the config
   * @param fullConfig full config
   * @param spec connector specification
   * @param runtimeSecretPersistence runtime secret persistence
   * @return partial config
   */
  fun statefulSplitSecretsToRuntimeSecretPersistence(
    workspaceId: UUID,
    fullConfig: JsonNode,
    spec: ConnectorSpecification,
    runtimeSecretPersistence: RuntimeSecretPersistence,
  ): JsonNode {
    return splitSecretConfig(workspaceId, fullConfig, spec, runtimeSecretPersistence)
  }
  // todo (cgardens) - the contract on this method is hard to follow, because it sometimes returns
  // secrets (i.e. when there is no longLivedSecretPersistence). If we treated all secrets the same
  // (i.e. used a separate db for secrets when the user didn't provide a store), this would be easier
  // to reason about.

  /**
   * If a secrets store is present, this method attempts to fetch the existing config and merge its
   * secrets with the passed in config. If there is no secrets store, it just returns the passed in
   * config. Also validates the config.
   *
   * Uses the environment secret persistence to store secrets.
   *
   * @param workspaceId workspace id for the config
   * @param oldConfig old full config
   * @param fullConfig new full config
   * @param spec connector specification
   * @param validate should the spec be validated, tombstone entries should not be validated
   * @return partial config
   */
  @Throws(JsonValidationException::class)
  fun statefulUpdateSecretsToDefaultSecretPersistence(
    workspaceId: UUID,
    oldConfig: Optional<JsonNode>,
    fullConfig: JsonNode,
    spec: JsonNode,
    validate: Boolean,
  ): JsonNode {
    if (validate) {
      validator.ensure(spec, fullConfig)
    }
    val splitSecretConfig: SplitSecretConfig =
      if (oldConfig.isPresent) {
        SecretsHelpers.splitAndUpdateConfig(
          workspaceId,
          oldConfig.get(),
          fullConfig,
          spec,
          secretPersistence,
        )
      } else {
        SecretsHelpers.splitConfig(
          workspaceId,
          fullConfig,
          spec,
          secretPersistence,
        )
      }
    splitSecretConfig.getCoordinateToPayload()
      .forEach { (coordinate: SecretCoordinate, payload: String) ->
        secretPersistence.write(coordinate, payload)
      }
    return splitSecretConfig.partialConfig
  }

  /**
   * If a secrets store is present, this method attempts to fetch the existing config and merge its
   * secrets with the passed in config. If there is no secrets store, it just returns the passed in
   * config. Also validates the config.
   *
   * Uses the provided runtime Secrets Persistence to store secrets.
   *
   * @param workspaceId workspace id for the config
   * @param oldConfig old full config
   * @param fullConfig new full config
   * @param spec connector specification
   * @param validate should the spec be validated, tombstone entries should not be validated
   * @return partial config
   */
  @Throws(JsonValidationException::class)
  fun statefulUpdateSecretsToRuntimeSecretPersistence(
    workspaceId: UUID,
    oldConfig: Optional<JsonNode>,
    fullConfig: JsonNode,
    spec: JsonNode,
    validate: Boolean,
    runtimeSecretPersistence: RuntimeSecretPersistence,
  ): JsonNode {
    if (validate) {
      validator.ensure(spec, fullConfig)
    }
    val splitSecretConfig: SplitSecretConfig =
      if (oldConfig.isPresent) {
        SecretsHelpers.splitAndUpdateConfig(
          workspaceId,
          oldConfig.get(),
          fullConfig,
          spec,
          runtimeSecretPersistence,
        )
      } else {
        SecretsHelpers.splitConfig(
          workspaceId,
          fullConfig,
          spec,
          runtimeSecretPersistence,
        )
      }
    splitSecretConfig.getCoordinateToPayload()
      .forEach { (coordinate: SecretCoordinate, payload: String) ->
        runtimeSecretPersistence.write(coordinate, payload)
      }
    return splitSecretConfig.partialConfig
  }

  /**
   * Takes in a connector configuration with secrets. Saves the secrets and returns the configuration
   * object with the secrets removed and replaced with pointers to the environment secret persistence.
   *
   * @param fullConfig full config
   * @param spec connector specification
   * @return partial config
   */
  fun statefulSplitSecretsToDefaultSecretPersistence(
    fullConfig: JsonNode,
    spec: ConnectorSpecification,
  ): JsonNode {
    return splitSecretConfig(NO_WORKSPACE, fullConfig, spec, secretPersistence)
  }

  /**
   * Takes in a connector configuration with secrets. Saves the secrets and returns the configuration
   * object with the secrets removed and replaced with pointers to the provided runtime secret persistence.
   *
   * @param fullConfig full config
   * @param spec connector specification
   * @param runtimeSecretPersistence runtime secret persistence
   * @return partial config
   */
  fun statefulSplitSecretsToRuntimeSecretPersistence(
    fullConfig: JsonNode,
    spec: ConnectorSpecification,
    runtimeSecretPersistence: RuntimeSecretPersistence,
  ): JsonNode {
    return splitSecretConfig(NO_WORKSPACE, fullConfig, spec, runtimeSecretPersistence)
  }

  private fun splitSecretConfig(
    workspaceId: UUID,
    fullConfig: JsonNode,
    spec: ConnectorSpecification,
    secretPersistence: SecretPersistence,
  ): JsonNode {
    val splitSecretConfig: SplitSecretConfig =
      SecretsHelpers.splitConfig(
        workspaceId,
        fullConfig,
        spec.connectionSpecification,
        secretPersistence,
      )
    splitSecretConfig.getCoordinateToPayload().forEach { (coordinate: SecretCoordinate, payload: String) ->
      secretPersistence.write(coordinate, payload)
    }
    return splitSecretConfig.partialConfig
  }

  /**
   * No frills, given a coordinate, just store the payload. Uses the environment secret persistence.
   */
  fun storeSecretToDefaultSecretPersistence(
    secretCoordinate: SecretCoordinate,
    payload: String,
  ): SecretCoordinate {
    secretPersistence.write(secretCoordinate, payload)
    return secretCoordinate
  }

  /**
   * No frills, given a coordinate, just store the payload in the provided runtime secret persistence.
   */
  fun storeSecretToRuntimeSecretPersistence(
    secretCoordinate: SecretCoordinate,
    payload: String,
    runtimeSecretPersistence: RuntimeSecretPersistence,
  ): SecretCoordinate {
    runtimeSecretPersistence.write(secretCoordinate, payload)
    return secretCoordinate
  }

  companion object {
    private val NO_WORKSPACE = UUID.fromString("00000000-0000-0000-0000-000000000000")
  }
}
