/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.JsonPaths
import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence
import io.airbyte.config.secrets.persistence.SecretPersistence
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.metrics.lib.MetricTags.SECRET_COORDINATES_UPDATED
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
   * @param workspaceId the workspace id for the config
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
  ): JsonNode =
    createFromConfig(workspaceId, fullConfig, connSpec, runtimeSecretPersistence, AirbyteManagedSecretCoordinate.DEFAULT_SECRET_BASE_PREFIX)

  /**
   * Detects secrets in the configuration. Writes them to the secrets store. It returns the config
   * stripped of secrets (replaced with pointers to the secrets store).
   *
   * Uses the environment secret persistence if needed.
   *
   * @param secretBaseId the id for the config
   * @param fullConfig full config
   * @param connSpec connector specification
   * @param runtimeSecretPersistence to use as an override
   * @param secretBasePrefix the base prefix of the secret (airbyte_workspace_ by default)
   * @return partial config
   */
  fun createFromConfig(
    secretBaseId: UUID,
    fullConfig: JsonNode,
    connSpec: JsonNode,
    runtimeSecretPersistence: RuntimeSecretPersistence? = null,
    secretBasePrefix: String = AirbyteManagedSecretCoordinate.DEFAULT_SECRET_BASE_PREFIX,
  ): JsonNode {
    val activePersistence = runtimeSecretPersistence ?: secretPersistence
    return splitSecretConfig(secretBaseId, secretBasePrefix, fullConfig, connSpec, activePersistence)
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
        SecretsHelpers.getExistingCoordinateIfExists(jsonWithCoordinate)?.let { coordinateText ->
          // Only delete secrets that are managed by Airbyte.
          AirbyteManagedSecretCoordinate.fromFullCoordinate(coordinateText)?.let { airbyteManagedCoordinate ->
            logger.info { "Deleting: ${airbyteManagedCoordinate.fullCoordinate}" }
            try {
              (runtimeSecretPersistence ?: secretPersistence).delete(airbyteManagedCoordinate)
              metricClient.count(
                metric = OssMetricsRegistry.DELETE_SECRET_DEFAULT_STORE,
                attributes = arrayOf(MetricAttribute(MetricTags.SUCCESS, "true")),
              )
            } catch (e: Exception) {
              // Multiple versions within one secret is a legacy concern. This is no longer
              // possible moving forward. Catch the exception to best-effort disable other secret versions.
              // The other reason to catch this is propagating the exception prevents the database
              // from being updated with the new coordinates.
              metricClient.count(
                metric = OssMetricsRegistry.DELETE_SECRET_DEFAULT_STORE,
                attributes = arrayOf(MetricAttribute(MetricTags.SUCCESS, "false")),
              )
              logger.error(e) { "Error deleting secret: ${airbyteManagedCoordinate.fullCoordinate}" }
            }
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
   * @param workspaceId the workspace id for the config
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
  ): JsonNode =
    updateFromConfig(
      workspaceId,
      oldPartialConfig,
      fullConfig,
      spec,
      runtimeSecretPersistence,
      AirbyteManagedSecretCoordinate.DEFAULT_SECRET_BASE_PREFIX,
    )

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
   * @param secretBaseId the id for the config
   * @param oldPartialConfig old partial config (no secrets)
   * @param fullConfig new full config (with secrets)
   * @param spec connector specification
   * @param runtimeSecretPersistence to use as an override
   * @param secretBasePrefix the base prefix of the secret (airbyte_workspace_ by default). This value is only used if there when no existing coordinates
   * @return partial config
   */
  @Throws(JsonValidationException::class)
  fun updateFromConfig(
    secretBaseId: UUID,
    oldPartialConfig: JsonNode,
    fullConfig: JsonNode,
    spec: JsonNode,
    runtimeSecretPersistence: RuntimeSecretPersistence? = null,
    secretBasePrefix: String = AirbyteManagedSecretCoordinate.DEFAULT_SECRET_BASE_PREFIX,
  ): JsonNode {
    validator.ensure(spec, fullConfig)

    val updatedSplitConfig: SplitSecretConfig =
      SecretsHelpers.splitAndUpdateConfig(secretBaseId, oldPartialConfig, fullConfig, spec, secretPersistence, secretBasePrefix)

    updatedSplitConfig
      .getCoordinateToPayload()
      .forEach { (coordinate: AirbyteManagedSecretCoordinate, payload: String) ->
        runtimeSecretPersistence?.write(coordinate, payload) ?: secretPersistence.write(coordinate, payload)
        metricClient.count(metric = OssMetricsRegistry.UPDATE_SECRET_DEFAULT_STORE)
      }
    updatedSplitConfig.getCoordinateToPayload().map { it.key }.let {
      ApmTraceUtils.addTagsToTrace(mapOf(SECRET_COORDINATES_UPDATED to it))
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
      AirbyteManagedSecretCoordinate.DEFAULT_SECRET_BASE_ID,
      AirbyteManagedSecretCoordinate.DEFAULT_SECRET_BASE_PREFIX,
      fullConfig,
      connSpec,
      activePersistence,
      Instant.now().plus(EPHEMERAL_SECRET_LIFE_DURATION),
    )
  }

  private fun splitSecretConfig(
    secretBaseId: UUID,
    secretBasePrefix: String,
    fullConfig: JsonNode,
    connSpec: JsonNode,
    secretPersistence: SecretPersistence,
    expireTime: Instant? = null,
  ): JsonNode {
    val splitSecretConfig: SplitSecretConfig =
      SecretsHelpers.splitConfig(
        secretBaseId,
        fullConfig,
        connSpec,
        secretPersistence,
        secretBasePrefix,
      )
    // modify this to add expire time
    splitSecretConfig.getCoordinateToPayload().forEach { (coordinate: AirbyteManagedSecretCoordinate, payload: String) ->
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
    coordinate: AirbyteManagedSecretCoordinate,
    payload: String,
    runtimeSecretPersistence: RuntimeSecretPersistence? = null,
  ): AirbyteManagedSecretCoordinate {
    runtimeSecretPersistence?.write(coordinate, payload) ?: secretPersistence.write(coordinate, payload)
    return coordinate
  }
}
