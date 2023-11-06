/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.yaml.Yamls
import io.airbyte.config.ConfigSchema
import io.airbyte.config.DestinationConnection
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.WorkspaceServiceAccount
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.config.persistence.ConfigRepository
import io.airbyte.config.secrets.persistence.SecretPersistence
import io.airbyte.protocol.models.ConnectorSpecification
import io.airbyte.validation.json.JsonSchemaValidator
import io.airbyte.validation.json.JsonValidationException
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import java.io.IOException
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
@Requires(bean = ConfigRepository::class)
@Requires(bean = SecretPersistence::class)
open class SecretsRepositoryWriter(
  private val configRepository: ConfigRepository,
  private val validator: JsonSchemaValidator,
  private val secretPersistence: SecretPersistence,
) {
  @Throws(JsonValidationException::class, IOException::class)
  private fun getSourceIfExists(sourceId: UUID): Optional<SourceConnection> {
    return try {
      Optional.of<SourceConnection>(configRepository.getSourceConnection(sourceId))
    } catch (e: ConfigNotFoundException) {
      logger.warn(e) { "Unable to find source with ID $sourceId." }
      Optional.empty<SourceConnection>()
    }
  }

  /**
   * Write a source with its secrets to the appropriate persistence. Secrets go to secrets store and
   * the rest of the object (with pointers to the secrets store) get saved in the db.
   *
   * @param source to write
   * @param connectorSpecification spec for the destination
   * @throws JsonValidationException if the workspace is or contains invalid json
   * @throws IOException if there is an issue while interacting with the secrets store or db.
   */
  @Throws(JsonValidationException::class, IOException::class)
  fun writeSourceConnection(
    source: SourceConnection,
    connectorSpecification: ConnectorSpecification,
  ) {
    val previousSourceConnection =
      getSourceIfExists(source.sourceId)
        .map { obj: SourceConnection -> obj.configuration }

    // strip secrets
    val partialConfig =
      statefulUpdateSecrets(
        source.workspaceId,
        previousSourceConnection,
        source.configuration,
        connectorSpecification.connectionSpecification,
        source.tombstone == null || !source.tombstone,
      )
    val partialSource: SourceConnection = Jsons.clone<SourceConnection>(source).withConfiguration(partialConfig)
    configRepository.writeSourceConnectionNoSecrets(partialSource)
  }

  @Throws(JsonValidationException::class, IOException::class)
  private fun getDestinationIfExists(destinationId: UUID): Optional<DestinationConnection> {
    return try {
      return Optional.of(configRepository.getDestinationConnection(destinationId))
    } catch (e: ConfigNotFoundException) {
      logger.warn(e) { "Unable to find destination with ID $destinationId." }
      Optional.empty<DestinationConnection>()
    }
  }

  /**
   * Write a destination with its secrets to the appropriate persistence. Secrets go to secrets store
   * and the rest of the object (with pointers to the secrets store) get saved in the db.
   *
   * @param destination to write
   * @param connectorSpecification spec for the destination
   * @throws JsonValidationException if the workspace is or contains invalid json
   * @throws IOException if there is an issue while interacting with the secrets store or db.
   */
  @Throws(JsonValidationException::class, IOException::class)
  fun writeDestinationConnection(
    destination: DestinationConnection,
    connectorSpecification: ConnectorSpecification,
  ) {
    val previousDestinationConnection =
      getDestinationIfExists(destination.destinationId)
        .map { obj: DestinationConnection -> obj.configuration }
    val partialConfig =
      statefulUpdateSecrets(
        destination.workspaceId,
        previousDestinationConnection,
        destination.configuration,
        connectorSpecification.connectionSpecification,
        destination.tombstone == null || !destination.tombstone,
      )
    val partialDestination: DestinationConnection =
      Jsons.clone(destination).withConfiguration(partialConfig)
    configRepository.writeDestinationConnectionNoSecrets(partialDestination)
  }

  /**
   * Detects secrets in the configuration. Writes them to the secrets store. It returns the config
   * stripped of secrets (replaced with pointers to the secrets store).
   *
   * @param workspaceId workspace id for the config
   * @param fullConfig full config
   * @param spec connector specification
   * @return partial config
   */
  fun statefulSplitSecrets(
    workspaceId: UUID,
    fullConfig: JsonNode,
    spec: ConnectorSpecification,
  ): JsonNode {
    return splitSecretConfig(workspaceId, fullConfig, spec, secretPersistence)
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
   * @param workspaceId workspace id for the config
   * @param oldConfig old full config
   * @param fullConfig new full config
   * @param spec connector specification
   * @param validate should the spec be validated, tombstone entries should not be validated
   * @return partial config
   */
  @Throws(JsonValidationException::class)
  private fun statefulUpdateSecrets(
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
        )
      }
    splitSecretConfig.getCoordinateToPayload()
      .forEach { (coordinate: SecretCoordinate, payload: String) ->
        secretPersistence.write(coordinate, payload)
      }
    return splitSecretConfig.partialConfig
  }

  /**
   * Takes in a connector configuration with secrets. Saves the secrets and returns the configuration
   * object with the secrets removed and replaced with pointers to the secrets store.
   *
   * @param fullConfig full config
   * @param spec connector specification
   * @return partial config
   */
  fun statefulSplitEphemeralSecrets(
    fullConfig: JsonNode,
    spec: ConnectorSpecification,
  ): JsonNode {
    return splitSecretConfig(NO_WORKSPACE, fullConfig, spec, secretPersistence)
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
      )
    splitSecretConfig.getCoordinateToPayload().forEach { (coordinate: SecretCoordinate, payload: String) ->
      secretPersistence.write(coordinate, payload)
    }
    return splitSecretConfig.partialConfig
  }

  /**
   * Write a service account with its secrets to the appropriate persistence. Secrets go to secrets
   * store and the rest of the object (with pointers to the secrets store) get saved in the db.
   *
   * @param workspaceServiceAccount to write
   * @throws JsonValidationException if the workspace is or contains invalid json
   * @throws IOException if there is an issue while interacting with the secrets store or db.
   */
  @Throws(JsonValidationException::class, IOException::class)
  fun writeServiceAccountJsonCredentials(workspaceServiceAccount: WorkspaceServiceAccount) {
    val workspaceServiceAccountForDB: WorkspaceServiceAccount =
      getWorkspaceServiceAccountWithSecretCoordinate(workspaceServiceAccount)
    configRepository.writeWorkspaceServiceAccountNoSecrets(workspaceServiceAccountForDB)
  }

  /**
   * This method is to encrypt the secret JSON key and HMAC key of a GCP service account a associated
   * with a workspace. If in future we build a similar feature i.e. an AWS account associated with a
   * workspace, we will have to build new implementation for it
   */
  @Throws(JsonValidationException::class, IOException::class)
  private fun getWorkspaceServiceAccountWithSecretCoordinate(workspaceServiceAccount: WorkspaceServiceAccount): WorkspaceServiceAccount {
    val clonedWorkspaceServiceAccount: WorkspaceServiceAccount =
      Jsons.clone(workspaceServiceAccount)
    val optionalWorkspaceServiceAccount: Optional<WorkspaceServiceAccount> =
      getOptionalWorkspaceServiceAccount(
        workspaceServiceAccount.workspaceId,
      )
    // Convert the JSON key of Service Account into secret co-oridnate. Ref :
    // https://cloud.google.com/iam/docs/service-accounts#key-types
    if (workspaceServiceAccount.jsonCredential != null) {
      val jsonCredSecretCoordinateToPayload: SecretCoordinateToPayload =
        SecretsHelpers.convertServiceAccountCredsToSecret(
          workspaceServiceAccount.jsonCredential.toPrettyString(),
          secretPersistence,
          workspaceServiceAccount.workspaceId,
          { UUID.randomUUID() },
          optionalWorkspaceServiceAccount.map { obj: WorkspaceServiceAccount -> obj.jsonCredential }
            .orElse(null),
          "json",
        )
      secretPersistence.write(
        jsonCredSecretCoordinateToPayload.secretCoordinate,
        jsonCredSecretCoordinateToPayload.payload,
      )
      clonedWorkspaceServiceAccount.jsonCredential = jsonCredSecretCoordinateToPayload.secretCoordinateForDB
    }
    // Convert the HMAC key of Service Account into secret coordinate. Ref :
    // https://cloud.google.com/storage/docs/authentication/hmackeys
    if (workspaceServiceAccount.hmacKey != null) {
      val hmackKeySecretCoordinateToPayload: SecretCoordinateToPayload =
        SecretsHelpers.convertServiceAccountCredsToSecret(
          workspaceServiceAccount.hmacKey.toString(),
          secretPersistence,
          workspaceServiceAccount.workspaceId,
          { UUID.randomUUID() },
          optionalWorkspaceServiceAccount.map { obj: WorkspaceServiceAccount -> obj.hmacKey }
            .orElse(null),
          "hmac",
        )
      secretPersistence.write(
        hmackKeySecretCoordinateToPayload.secretCoordinate,
        hmackKeySecretCoordinateToPayload.payload,
      )
      clonedWorkspaceServiceAccount.hmacKey = hmackKeySecretCoordinateToPayload.secretCoordinateForDB
    }
    return clonedWorkspaceServiceAccount
  }
  // todo (cgardens) - should a get method be here? this is a writer not a reader.

  /**
   * Get service account with no secrets.
   *
   * @param workspaceId workspace id
   * @return service account object with no secrets
   * @throws JsonValidationException if the workspace is or contains invalid json
   * @throws IOException if there is an issue while interacting with the secrets store or db.
   */
  @Throws(JsonValidationException::class, IOException::class)
  fun getOptionalWorkspaceServiceAccount(workspaceId: UUID?): Optional<WorkspaceServiceAccount> {
    return try {
      Optional.of<WorkspaceServiceAccount>(
        configRepository.getWorkspaceServiceAccountNoSecrets(
          workspaceId,
        ),
      )
    } catch (e: ConfigNotFoundException) {
      logger.warn(e) { "Unable to find workspace service account in workspace $workspaceId." }
      Optional.empty<WorkspaceServiceAccount>()
    }
  }

  /**
   * Write a workspace with its secrets to the appropriate persistence. Secrets go to secrets store
   * and the rest of the object (with pointers to the secrets store) get saved in the db.
   *
   * @param workspace to save
   * @throws JsonValidationException if the workspace is or contains invalid json
   * @throws IOException if there is an issue while interacting with the secrets store or db.
   */
  @Throws(JsonValidationException::class, IOException::class)
  fun writeWorkspace(workspace: StandardWorkspace) {
    // Get the schema for the webhook config, so we can split out any secret fields.
    val webhookConfigSchema: JsonNode =
      Yamls.deserialize(ConfigSchema.WORKSPACE_WEBHOOK_OPERATION_CONFIGS.configSchemaFile)
    // Check if there's an existing config, so we can re-use the secret coordinates.
    val previousWorkspace: Optional<StandardWorkspace> = getWorkspaceIfExists(workspace.workspaceId, false)
    var previousWebhookConfigs = Optional.empty<JsonNode>()
    if (previousWorkspace.isPresent && previousWorkspace.get().webhookOperationConfigs != null) {
      previousWebhookConfigs = Optional.of(previousWorkspace.get().webhookOperationConfigs)
    }
    // Split out the secrets from the webhook config.
    val partialConfig =
      if (workspace.webhookOperationConfigs == null) {
        null
      } else {
        statefulUpdateSecrets(
          workspace.workspaceId,
          previousWebhookConfigs,
          workspace.webhookOperationConfigs,
          webhookConfigSchema,
          true,
        )
      }
    val partialWorkspace: StandardWorkspace = Jsons.clone(workspace)
    if (partialConfig != null) {
      partialWorkspace.withWebhookOperationConfigs(partialConfig)
    }
    configRepository.writeStandardWorkspaceNoSecrets(partialWorkspace)
  }

  private fun getWorkspaceIfExists(
    workspaceId: UUID,
    includeTombstone: Boolean,
  ): Optional<StandardWorkspace> {
    return try {
      return Optional.ofNullable(configRepository.getStandardWorkspaceNoSecrets(workspaceId, includeTombstone))
    } catch (e: JsonValidationException) {
      logger.warn(e) { "Unable to find workspace with ID $workspaceId." }
      Optional.empty<StandardWorkspace>()
    } catch (e: IOException) {
      logger.warn(e) { "Unable to find workspace with ID $workspaceId." }
      Optional.empty<StandardWorkspace>()
    } catch (e: ConfigNotFoundException) {
      logger.warn(e) { "Unable to find workspace with ID $workspaceId." }
      Optional.empty<StandardWorkspace>()
    }
  }

  /**
   * No frills, given a coordinate, just store the payload.
   */
  fun storeSecret(
    secretCoordinate: SecretCoordinate,
    payload: String,
  ): SecretCoordinate {
    secretPersistence.write(secretCoordinate, payload)
    return secretCoordinate
  }

  companion object {
    private val NO_WORKSPACE = UUID.fromString("00000000-0000-0000-0000-000000000000")
  }
}
