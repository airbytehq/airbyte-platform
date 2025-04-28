/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence
import io.airbyte.config.secrets.persistence.SecretPersistence
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
   * @param secretPersistence to store the secrets
   * @return partial config
   */
  fun createFromConfig(
    workspaceId: UUID,
    fullConfig: ConfigWithProcessedSecrets,
    secretPersistence: SecretPersistence,
  ): JsonNode = createFromConfig(workspaceId, fullConfig, secretPersistence, AirbyteManagedSecretCoordinate.DEFAULT_SECRET_BASE_PREFIX)

  /**
   * Detects secrets in the configuration. Writes them to the secrets store. It returns the config
   * stripped of secrets (replaced with pointers to the secrets store).
   *
   * Uses the environment secret persistence if needed.
   *
   * @param secretBaseId the id for the config
   * @param fullConfig full config
   * @param secretPersistence to store the secrets
   * @param secretBasePrefix the base prefix of the secret (airbyte_workspace_ by default)
   * @return partial config
   */
  fun createFromConfig(
    secretBaseId: UUID,
    fullConfig: ConfigWithProcessedSecrets,
    secretPersistence: SecretPersistence,
    secretBasePrefix: String = AirbyteManagedSecretCoordinate.DEFAULT_SECRET_BASE_PREFIX,
  ): JsonNode = splitSecretConfig(secretBaseId, secretBasePrefix, fullConfig, secretPersistence)

  @Deprecated("Use createFromConfig() that takes in InputConfigWithProcessedSecrets instead")
  fun createFromConfigLegacy(
    secretBaseId: UUID,
    fullConfig: JsonNode,
    connSpec: JsonNode,
    runtimeSecretPersistence: RuntimeSecretPersistence? = null,
    secretBasePrefix: String,
  ): JsonNode {
    val fullConfigWithProcessedSecrets =
      SecretsHelpers.SecretReferenceHelpers.processConfigSecrets(
        actorConfig = fullConfig,
        spec = connSpec,
        secretStorageId = null,
      )
    return createFromConfig(
      secretBaseId = secretBaseId,
      fullConfig = fullConfigWithProcessedSecrets,
      secretPersistence = runtimeSecretPersistence ?: secretPersistence,
      secretBasePrefix = secretBasePrefix,
    )
  }

  @Deprecated("Use createFromConfig() that takes in InputConfigWithProcessedSecrets instead")
  fun createFromConfigLegacy(
    secretBaseId: UUID,
    fullConfig: JsonNode,
    connSpec: JsonNode,
    runtimeSecretPersistence: RuntimeSecretPersistence? = null,
  ): JsonNode =
    createFromConfigLegacy(
      secretBaseId = secretBaseId,
      fullConfig = fullConfig,
      connSpec = connSpec,
      runtimeSecretPersistence = runtimeSecretPersistence,
      secretBasePrefix = AirbyteManagedSecretCoordinate.DEFAULT_SECRET_BASE_PREFIX,
    )

  /**
   * Pure function to delete secrets from persistence.
   *
   * @param config secret config to be deleted
   * @param secretPersistence where secrets are stored
   */
  @Throws(JsonValidationException::class)
  fun deleteFromConfig(
    config: ConfigWithSecretReferences,
    secretPersistence: SecretPersistence,
  ) {
    config.referencedSecrets.forEach { (_, secretReferenceConfig) ->
      val secretCoordinate = secretReferenceConfig.secretCoordinate
      // Only delete secrets that are managed by Airbyte, for organizations that aren't yet using secret references
      if (secretCoordinate is AirbyteManagedSecretCoordinate && secretReferenceConfig.secretStorageId == null) {
        deleteAirbyteManagedSecretCoordinate(secretCoordinate, secretPersistence)
      }
    }
    logger.info { "Deleting secrets done!" }
  }

  private fun deleteAirbyteManagedSecretCoordinate(
    secretCoordinate: AirbyteManagedSecretCoordinate,
    secretPersistence: SecretPersistence,
  ) {
    logger.info { "Deleting: ${secretCoordinate.fullCoordinate}" }
    try {
      secretPersistence.delete(secretCoordinate)
      metricClient.count(
        metric = OssMetricsRegistry.DELETE_SECRET_DEFAULT_STORE,
        attributes = arrayOf(MetricAttribute(MetricTags.SUCCESS, "true")),
      )
    } catch (e: Exception) {
      metricClient.count(
        metric = OssMetricsRegistry.DELETE_SECRET_DEFAULT_STORE,
        attributes = arrayOf(MetricAttribute(MetricTags.SUCCESS, "false")),
      )
      logger.error(e) { "Error deleting secret: ${secretCoordinate.fullCoordinate}" }
    }
  }

  @Deprecated("Use deleteFromConfig() that takes in ConfigWithSecretReferences instead")
  @Throws(JsonValidationException::class)
  fun deleteFromConfig(
    config: JsonNode,
    runtimeSecretPersistence: RuntimeSecretPersistence? = null,
  ) {
    val configWithSecretRefs = buildConfigWithSecretRefsJava(config)
    deleteFromConfig(configWithSecretRefs, runtimeSecretPersistence ?: secretPersistence)
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
   * @param secretPersistence where secrets are stored
   * @return partial config
   */
  @Throws(JsonValidationException::class)
  fun updateFromConfig(
    workspaceId: UUID,
    oldPartialConfig: ConfigWithSecretReferences,
    fullConfig: ConfigWithProcessedSecrets,
    spec: JsonNode,
    secretPersistence: SecretPersistence,
  ): JsonNode =
    updateFromConfig(
      workspaceId,
      oldPartialConfig,
      fullConfig,
      spec,
      secretPersistence,
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
   * @param secretPersistence where secrets are stored
   * @param secretBasePrefix the base prefix of the secret (airbyte_workspace_ by default). This value is only used if there when no existing coordinates
   * @return partial config
   */
  @Throws(JsonValidationException::class)
  fun updateFromConfig(
    secretBaseId: UUID,
    oldPartialConfig: ConfigWithSecretReferences,
    fullConfig: ConfigWithProcessedSecrets,
    spec: JsonNode,
    secretPersistence: SecretPersistence,
    secretBasePrefix: String = AirbyteManagedSecretCoordinate.DEFAULT_SECRET_BASE_PREFIX,
  ): JsonNode {
    val configWithSecretPlaceholders =
      SecretsHelpers.SecretReferenceHelpers.configWithTextualSecretPlaceholders(
        fullConfig.originalConfig,
        spec,
      )
    validator.ensure(spec, configWithSecretPlaceholders)

    val updatedSplitConfig: SplitSecretConfig =
      SecretsHelpers.splitAndUpdateConfig(secretBaseId, oldPartialConfig, fullConfig, secretPersistence, secretBasePrefix)

    updatedSplitConfig
      .getCoordinateToPayload()
      .forEach { (coordinate: AirbyteManagedSecretCoordinate, payload: String) ->
        secretPersistence.write(coordinate, payload)
        metricClient.count(metric = OssMetricsRegistry.UPDATE_SECRET_DEFAULT_STORE)
      }
    updatedSplitConfig.getCoordinateToPayload().map { it.key }.let {
      ApmTraceUtils.addTagsToTrace(mapOf(SECRET_COORDINATES_UPDATED to it))
    }

    deleteLegacyAirbyteManagedCoordinates(oldPartialConfig, fullConfig, secretPersistence)

    return updatedSplitConfig.partialConfig
  }

  /**
   * For legacy configs not yet using secret references, delete old airbyte-managed secrets that
   * are no longer relevant after the update. Legacy configs are those that do not have an
   * associated secretStorageId.
   */
  private fun deleteLegacyAirbyteManagedCoordinates(
    oldPartialConfig: ConfigWithSecretReferences,
    fullConfig: ConfigWithProcessedSecrets,
    secretPersistence: SecretPersistence,
  ) {
    oldPartialConfig.referencedSecrets.forEach { (path, oldSecretRef) ->
      (oldSecretRef.secretCoordinate as? AirbyteManagedSecretCoordinate)
        ?.takeIf { fullConfig.processedSecrets[path]?.rawValue != null }
        ?.takeIf { oldSecretRef.secretStorageId == null }
        ?.let { deleteAirbyteManagedSecretCoordinate(it, secretPersistence) }
    }
  }

  @Deprecated("Use updateFromConfig() that takes in ConfigWithSecretReferences instead")
  @Throws(JsonValidationException::class)
  fun updateFromConfigLegacy(
    secretBaseId: UUID,
    oldPartialConfig: JsonNode,
    fullConfig: JsonNode,
    spec: JsonNode,
    runtimeSecretPersistence: RuntimeSecretPersistence? = null,
    secretBasePrefix: String = AirbyteManagedSecretCoordinate.DEFAULT_SECRET_BASE_PREFIX,
  ): JsonNode {
    val oldPartialConfigWithSecretReferences = buildConfigWithSecretRefsJava(oldPartialConfig)
    val fullConfigWithProcessedSecrets =
      SecretsHelpers.SecretReferenceHelpers.processConfigSecrets(
        actorConfig = fullConfig,
        spec = spec,
        secretStorageId = null,
      )

    return updateFromConfig(
      secretBaseId = secretBaseId,
      oldPartialConfig = oldPartialConfigWithSecretReferences,
      fullConfig = fullConfigWithProcessedSecrets,
      spec = spec,
      secretPersistence = runtimeSecretPersistence ?: secretPersistence,
      secretBasePrefix = secretBasePrefix,
    )
  }

  @Deprecated("Use updateFromConfig() that takes in ConfigWithSecretReferences instead")
  @Throws(JsonValidationException::class)
  fun updateFromConfigLegacy(
    secretBaseId: UUID,
    oldPartialConfig: JsonNode,
    fullConfig: JsonNode,
    spec: JsonNode,
    runtimeSecretPersistence: RuntimeSecretPersistence? = null,
  ): JsonNode =
    updateFromConfigLegacy(
      secretBaseId = secretBaseId,
      oldPartialConfig = oldPartialConfig,
      fullConfig = fullConfig,
      spec = spec,
      runtimeSecretPersistence = runtimeSecretPersistence,
      secretBasePrefix = AirbyteManagedSecretCoordinate.DEFAULT_SECRET_BASE_PREFIX,
    )

  /**
   * Takes in a connector configuration with secrets. Saves the secrets and returns the configuration
   * object with the secrets removed and replaced with pointers to the environment secret persistence.
   *
   * This method is intended for ephemeral secrets, hence the lack of workspace.
   *
   * Ephemeral secrets are intended to be expired after a certain duration for cost and security reasons.
   *
   * @param fullConfig full config
   * @param secretPersistence to store the secrets
   * @return partial config
   */
  fun createEphemeralFromConfig(
    fullConfig: ConfigWithProcessedSecrets,
    secretPersistence: SecretPersistence,
  ): JsonNode =
    splitSecretConfig(
      AirbyteManagedSecretCoordinate.DEFAULT_SECRET_BASE_ID,
      AirbyteManagedSecretCoordinate.DEFAULT_SECRET_BASE_PREFIX,
      fullConfig,
      secretPersistence,
      Instant.now().plus(EPHEMERAL_SECRET_LIFE_DURATION),
    )

  private fun splitSecretConfig(
    secretBaseId: UUID,
    secretBasePrefix: String,
    fullConfig: ConfigWithProcessedSecrets,
    secretPersistence: SecretPersistence,
    expireTime: Instant? = null,
  ): JsonNode {
    val splitSecretConfig: SplitSecretConfig =
      SecretsHelpers.splitConfig(
        secretBaseId,
        fullConfig,
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
   * No frills, given a coordinate, just store the payload. Uses the provided secret persistence.
   */
  fun store(
    coordinate: AirbyteManagedSecretCoordinate,
    payload: String,
    secretPersistence: SecretPersistence,
  ): AirbyteManagedSecretCoordinate {
    secretPersistence.write(coordinate, payload)
    return coordinate
  }

  fun storeInDefaultPersistence(
    coordinate: AirbyteManagedSecretCoordinate,
    payload: String,
  ): AirbyteManagedSecretCoordinate {
    secretPersistence.write(coordinate, payload)
    return coordinate
  }
}
