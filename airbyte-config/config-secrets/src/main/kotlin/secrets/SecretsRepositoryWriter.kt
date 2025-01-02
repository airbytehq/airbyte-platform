/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.JsonPaths
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence
import io.airbyte.config.secrets.persistence.SecretPersistence
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.MetricTags
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
   * @param connSpec connector specification
   * @param runtimeSecretPersistence to use as an override
   * @return partial config
   */
  fun createFromConfig(
    workspaceId: UUID,
    fullConfig: JsonNode,
    connSpec: JsonNode,
    runtimeSecretPersistence: RuntimeSecretPersistence? = null,
  ): JsonNode {
    val activePersistence = runtimeSecretPersistence ?: secretPersistence
    return splitSecretConfig(workspaceId, fullConfig, connSpec, activePersistence)
  }

  /**
   * Pure function to delete secrets from persistence.
   *
   * @param config secret config to be deleted
   * @param spec connector specification
   * @param runtimeSecretPersistence to use as an override
   */
  @Throws(JsonValidationException::class)
  fun deleteFromConfig(
    config: JsonNode,
    spec: JsonNode,
    runtimeSecretPersistence: RuntimeSecretPersistence? = null,
  ) {
    val pathToSecrets = SecretsHelpers.getSortedSecretPaths(spec)
    pathToSecrets.forEach { path ->
      JsonPaths.getValues(config, path).forEach { jsonWithCoordinate ->
        SecretsHelpers.getExistingCoordinateIfExists(jsonWithCoordinate)?.let { coordinate ->
          val secretCoord = SecretCoordinate.fromFullCoordinate(coordinate)
          logger.info { "Deleting: ${secretCoord.fullCoordinate}" }
          try {
            (runtimeSecretPersistence ?: secretPersistence).delete(secretCoord)
            metricClient.count(OssMetricsRegistry.DELETE_SECRET_DEFAULT_STORE, 1, MetricAttribute(MetricTags.SUCCESS, "true"))
          } catch (e: Exception) {
            // Multiple versions within one secret is a legacy concern. This is no longer
            // possible moving forward. Catch the exception to best-effort disable other secret versions.
            // The other reason to catch this is propagating the exception prevents the database
            // from being updated with the new coordinates.
            metricClient.count(OssMetricsRegistry.DELETE_SECRET_DEFAULT_STORE, 1, MetricAttribute(MetricTags.SUCCESS, "false"))
            logger.error(e) { "Error deleting secret: ${secretCoord.fullCoordinate}" }
          }
        }
      }
    }
    logger.info { "Deleting secrets done!" }
  }

  /**
   * This method merges an existing partial config with a new full config. It writes the secrets to the
   * secrets store and returns the partial config with the secrets removed and replaced with secret coordinates.
   *
   * For simplicity, secrets are always written regardless of whether value change.
   *
   * Finally, delete the old secrets for cost and security considerations.
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
  fun updateFromConfig(
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
        runtimeSecretPersistence?.write(coordinate, payload) ?: secretPersistence.write(coordinate, payload)
        metricClient.count(OssMetricsRegistry.UPDATE_SECRET_DEFAULT_STORE, 1)
      }
    // Delete old secrets.
    deleteFromConfig(oldPartialConfig, spec, runtimeSecretPersistence)

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
   * @param connSpec connector specification
   * @param runtimeSecretPersistence to use as an override
   * @return partial config
   */
  fun createEphemeralFromConfig(
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
  fun store(
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
