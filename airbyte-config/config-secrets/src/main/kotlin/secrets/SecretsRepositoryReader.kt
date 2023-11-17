/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.lang.Exceptions
import io.airbyte.config.DestinationConnection
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.WorkspaceServiceAccount
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.config.persistence.ConfigRepository
import io.airbyte.config.secrets.hydration.SecretsHydrator
import io.airbyte.validation.json.JsonValidationException
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import java.io.IOException
import java.util.UUID
import java.util.stream.Collectors
import java.util.stream.Stream

private const val SECRET_KEY = "_secret"

/**
 * This class is responsible for fetching both connectors and their secrets (from separate secrets
 * stores). All methods in this class return secrets! Use it carefully.
 */
@Singleton
@Requires(bean = ConfigRepository::class)
@Requires(bean = SecretsHydrator::class)
open class SecretsRepositoryReader(
  private val configRepository: ConfigRepository,
  private val secretsHydrator: SecretsHydrator,
) {
  /**
   * Get source with secrets.
   *
   * @param sourceId source id
   * @return destination with secrets
   * @throws JsonValidationException if the workspace is or contains invalid json
   * @throws ConfigNotFoundException if the config does not exist
   * @throws IOException if there is an issue while interacting with the secrets store or db.
   */
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun getSourceConnectionWithSecrets(sourceId: UUID?): SourceConnection {
    val source = configRepository.getSourceConnection(sourceId)
    return hydrateSourcePartialConfig(source)
  }

  /**
   * List sources with secrets.
   *
   * @return sources with secrets
   * @throws JsonValidationException if the workspace is or contains invalid json
   * @throws IOException if there is an issue while interacting with the secrets store or db.
   */
  @Throws(JsonValidationException::class, IOException::class)
  fun listSourceConnectionWithSecrets(): List<SourceConnection> {
    val sources = configRepository.listSourceConnection()
    return sources
      .stream()
      .map { partialSource: SourceConnection ->
        Exceptions.toRuntime<SourceConnection> { hydrateSourcePartialConfig(partialSource) }
      }
      .collect(Collectors.toList())
  }

  /**
   * Get destination with secrets.
   *
   * @param destinationId destination id
   * @return destination with secrets
   * @throws JsonValidationException if the workspace is or contains invalid json
   * @throws ConfigNotFoundException if the config does not exist
   * @throws IOException if there is an issue while interacting with the secrets store or db.
   */
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun getDestinationConnectionWithSecrets(destinationId: UUID?): DestinationConnection {
    val destination = configRepository.getDestinationConnection(destinationId)
    return hydrateDestinationPartialConfig(destination)
  }

  /**
   * List destinations with secrets.
   *
   * @return destinations with secrets
   * @throws JsonValidationException if the workspace is or contains invalid json
   * @throws IOException if there is an issue while interacting with the secrets store or db.
   */
  @Throws(JsonValidationException::class, IOException::class)
  fun listDestinationConnectionWithSecrets(): List<DestinationConnection> {
    val destinations = configRepository.listDestinationConnection()
    return destinations
      .stream()
      .map { partialDestination: DestinationConnection ->
        Exceptions.toRuntime<DestinationConnection> {
          hydrateDestinationPartialConfig(
            partialDestination,
          )
        }
      }
      .collect(Collectors.toList())
  }

  private fun hydrateSourcePartialConfig(sourceWithPartialConfig: SourceConnection): SourceConnection {
    val hydratedConfig = secretsHydrator.hydrate(sourceWithPartialConfig.configuration)
    return Jsons.clone(sourceWithPartialConfig).withConfiguration(hydratedConfig)
  }

  private fun hydrateDestinationPartialConfig(sourceWithPartialConfig: DestinationConnection): DestinationConnection {
    val hydratedConfig = secretsHydrator.hydrate(sourceWithPartialConfig.configuration)
    return Jsons.clone(sourceWithPartialConfig).withConfiguration(hydratedConfig)
  }

  @Suppress("unused")
  private fun hydrateValuesIfKeyPresent(
    key: String,
    dump: MutableMap<String, Stream<JsonNode>>,
  ) {
    if (dump.containsKey(key)) {
      val augmentedValue =
        dump[key]!!.map { partialConfig: JsonNode ->
          secretsHydrator.hydrate(partialConfig)
        }
      dump[key] = augmentedValue
    }
  }

  /**
   * Get workspace service account with secrets.
   *
   * @param workspaceId workspace id
   * @return workspace service account with secrets
   * @throws JsonValidationException if the workspace is or contains invalid json
   * @throws ConfigNotFoundException if the config does not exist
   * @throws IOException if there is an issue while interacting with the secrets store or db.
   */
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun getWorkspaceServiceAccountWithSecrets(workspaceId: UUID?): WorkspaceServiceAccount {
    val workspaceServiceAccount = configRepository.getWorkspaceServiceAccountNoSecrets(workspaceId)
    val jsonCredential =
      if (workspaceServiceAccount.jsonCredential != null) {
        secretsHydrator.hydrateSecretCoordinate(
          workspaceServiceAccount.jsonCredential,
        )
      } else {
        null
      }
    val hmacKey =
      if (workspaceServiceAccount.hmacKey != null) secretsHydrator.hydrateSecretCoordinate(workspaceServiceAccount.hmacKey) else null
    return Jsons.clone(workspaceServiceAccount).withJsonCredential(jsonCredential).withHmacKey(hmacKey)
  }

  /**
   * Get workspace with secrets.
   *
   * @param workspaceId workspace id
   * @param includeTombstone include workspace even if it is tombstoned
   * @return workspace with secrets
   * @throws JsonValidationException if the workspace is or contains invalid json
   * @throws ConfigNotFoundException if the config does not exist
   * @throws IOException if there is an issue while interacting with the secrets store or db.
   */
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun getWorkspaceWithSecrets(
    workspaceId: UUID?,
    includeTombstone: Boolean,
  ): StandardWorkspace {
    val workspace = configRepository.getStandardWorkspaceNoSecrets(workspaceId, includeTombstone)
    val webhookConfigs = secretsHydrator.hydrate(workspace.webhookOperationConfigs)
    workspace.withWebhookOperationConfigs(webhookConfigs)
    return workspace
  }

  /**
   * Given a secret coordinate, fetch the secret.
   *
   * @param secretCoordinate secret coordinate
   * @return JsonNode representing the fetched secret
   */
  fun fetchSecret(secretCoordinate: SecretCoordinate): JsonNode {
    val node = JsonNodeFactory.instance.objectNode()
    node.put(SECRET_KEY, secretCoordinate.fullCoordinate)
    return secretsHydrator.hydrateSecretCoordinate(node)
  }

  /**
   * Given a config with _secrets in it, hydrate that config and return the hydrated version.
   *
   * @param configWithSecrets Config with _secrets in it.
   * @return Config with _secrets hydrated.
   */
  fun hydrateConfig(configWithSecrets: JsonNode?): JsonNode? {
    return if (configWithSecrets != null) {
      secretsHydrator.hydrate(configWithSecrets)
    } else {
      null
    }
  }
}
