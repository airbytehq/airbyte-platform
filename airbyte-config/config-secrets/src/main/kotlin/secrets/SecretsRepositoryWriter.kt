/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.JsonPaths
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence
import io.airbyte.config.secrets.persistence.SecretPersistence
import io.airbyte.featureflag.DeleteDanglingSecrets
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Workspace
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.OssMetricsRegistry
import io.airbyte.validation.json.JsonSchemaValidator
import io.airbyte.validation.json.JsonValidationException
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import java.time.Duration
import java.time.Instant
import java.util.UUID

private val logger = KotlinLogging.logger {}

private val EPHEMERAL_SECRET_LIFE_DURATION = Duration.ofHours(2)

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
  private val metricClient: MetricClient,
  private val featureFlagClient: FeatureFlagClient,
) {
  val validator: JsonSchemaValidator = JsonSchemaValidator()

  /**
   * Detects secrets in the configuration. Writes them to the secrets store. It returns the config
   * stripped of secrets (replaced with pointers to the secrets store).
   *
   * Uses the environment secret persistence if needed.
   *
   * @param workspaceId workspace id for the config
   * @param fullConfig full config
   * @param spec connector specification
   * @param runtimeSecretPersistence to use as an override
   * @return partial config
   */
  fun statefulSplitSecrets(
    workspaceId: UUID,
    fullConfig: JsonNode,
    connSpec: JsonNode,
    runtimeSecretPersistence: RuntimeSecretPersistence? = null,
  ): JsonNode {
    val activePersistence = runtimeSecretPersistence ?: secretPersistence
    return splitSecretConfig(workspaceId, fullConfig, connSpec, activePersistence)
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
   * Finally, delete the old versioned secrets.
   *
   * Uses the environment secret persistence if needed.
   *
   * @param workspaceId workspace id for the config
   * @param oldPartialConfig old partial config (no secrets)
   * @param fullConfig new full config (with secrets)
   * @param spec connector specification
   * @param runtimeSecretPersistence to use as an override
   * @return partial config
   */
  @Throws(JsonValidationException::class)
  fun statefulUpdateSecrets(
    workspaceId: UUID,
    oldPartialConfig: JsonNode,
    fullConfig: JsonNode,
    spec: JsonNode,
    runtimeSecretPersistence: RuntimeSecretPersistence? = null,
  ): JsonNode {
    validator.ensure(spec, fullConfig)

    val updatedSplitConfig: SplitSecretConfig =
      SecretsHelpers.splitAndUpdateConfig(workspaceId, oldPartialConfig, fullConfig, spec, secretPersistence)
    updatedSplitConfig.getCoordinateToPayload()
      .forEach { (coordinate: SecretCoordinate, payload: String) ->
        metricClient.count(OssMetricsRegistry.UPDATE_SECRET_DEFAULT_STORE, 1)
        runtimeSecretPersistence?.write(coordinate, payload) ?: secretPersistence.write(coordinate, payload)
      }

    SecretsHelpers.getSortedSecretPaths(spec).forEach { path ->
      JsonPaths.getValues(oldPartialConfig, path).forEach { jsonWithCoordinate ->
        SecretsHelpers.getExistingCoordinateIfExists(jsonWithCoordinate)?.let { coordinate ->
          featureFlagClient.boolVariation(DeleteDanglingSecrets, Workspace(workspaceId))
          val secretCoord = SecretCoordinate.fromFullCoordinate(coordinate)
          metricClient.count(OssMetricsRegistry.DELETE_SECRET_DEFAULT_STORE, 1)
          (runtimeSecretPersistence ?: secretPersistence).delete(secretCoord)
        }
      }
    }
    return updatedSplitConfig.partialConfig
  }

  /**
   * Takes in a connector configuration with secrets. Saves the secrets and returns the configuration
   * object with the secrets removed and replaced with pointers to the environment secret persistence.
   *
   * This method is intended for ephemeral secrets, hence the lack of workspace.
   *
   * Ephemeral secrets are intended to be expired after a certain duration for cost and security reasons.
   *
   * @param fullConfig full config
   * @param spec connector specification
   * @param runtimeSecretPersistence to use as an override
   * @return partial config
   */
  fun statefulSplitEphemeralSecrets(
    fullConfig: JsonNode,
    connSpec: JsonNode,
    runtimeSecretPersistence: RuntimeSecretPersistence? = null,
  ): JsonNode {
    val activePersistence = runtimeSecretPersistence ?: secretPersistence
    return splitSecretConfig(
      NO_WORKSPACE,
      fullConfig,
      connSpec,
      activePersistence,
      Instant.now().plus(EPHEMERAL_SECRET_LIFE_DURATION),
    )
  }

  private fun splitSecretConfig(
    workspaceId: UUID,
    fullConfig: JsonNode,
    connSpec: JsonNode,
    secretPersistence: SecretPersistence,
    expireTime: Instant? = null,
  ): JsonNode {
    val splitSecretConfig: SplitSecretConfig =
      SecretsHelpers.splitConfig(
        workspaceId,
        fullConfig,
        connSpec,
        secretPersistence,
      )
    // modify this to add expire time
    splitSecretConfig.getCoordinateToPayload().forEach { (coordinate: SecretCoordinate, payload: String) ->
      secretPersistence.writeWithExpiry(coordinate, payload, expireTime)
    }
    return splitSecretConfig.partialConfig
  }

  /**
   * No frills, given a coordinate, just store the payload. Uses the environment secret persistence.
   *
   * @param runtimeSecretPersistence to use as an override
   */
  fun storeSecret(
    coordinate: SecretCoordinate,
    payload: String,
    runtimeSecretPersistence: RuntimeSecretPersistence? = null,
  ): SecretCoordinate {
    runtimeSecretPersistence?.write(coordinate, payload) ?: secretPersistence.write(coordinate, payload)
    return coordinate
  }

  companion object {
    private val NO_WORKSPACE = UUID.fromString("00000000-0000-0000-0000-000000000000")
  }
}
