/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.config.DestinationConnection
import io.airbyte.config.SourceConnection
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.secrets.JsonSecretsProcessor
import io.airbyte.config.secrets.SecretsHelpers.mergeNodesExceptForSecrets
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.validation.json.JsonValidationException
import jakarta.inject.Singleton
import java.io.IOException
import java.util.Optional
import java.util.UUID

/**
 * Abstraction to manage the updating the configuration of a source or a destination. Helps with
 * secrets handling and make it easy to test these transitions.
 */
@Singleton
class ConfigurationUpdate(
  private val actorDefinitionVersionHelper: ActorDefinitionVersionHelper,
  private val sourceService: SourceService,
  private val destinationService: DestinationService,
) {
  val secretsProcessor: JsonSecretsProcessor = JsonSecretsProcessor(true)

  /**
   * Update the configuration object for a source.
   *
   * @param sourceId source id
   * @param sourceName name of source
   * @param newConfiguration new configuration
   * @return updated source configuration
   * @throws ConfigNotFoundException thrown if the source does not exist
   * @throws IOException thrown if exception while interacting with the db
   * @throws JsonValidationException thrown if newConfiguration is invalid json
   */
  @Throws(
    io.airbyte.config.persistence.ConfigNotFoundException::class,
    IOException::class,
    JsonValidationException::class,
    ConfigNotFoundException::class,
  )
  fun source(
    sourceId: UUID,
    sourceName: String?,
    newConfiguration: JsonNode,
  ): SourceConnection {
    // get existing source
    val persistedSource: SourceConnection
    try {
      persistedSource = sourceService.getSourceConnection(sourceId)
    } catch (e: ConfigNotFoundException) {
      throw io.airbyte.config.persistence
        .ConfigNotFoundException(e.type, e.configId)
    }
    persistedSource.name = sourceName
    // get spec
    val sourceDefinition = sourceService.getStandardSourceDefinition(persistedSource.sourceDefinitionId)
    val sourceVersion =
      actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, persistedSource.workspaceId, sourceId)
    val spec = sourceVersion.spec
    // copy any necessary secrets from the current source to the incoming updated source
    val updatedConfiguration =
      secretsProcessor.copySecrets(
        persistedSource.configuration,
        newConfiguration,
        spec.connectionSpecification,
      )

    return Jsons.clone(persistedSource).withConfiguration(updatedConfiguration)
  }

  /**
   * Partially update the configuration object for a source.
   *
   * @param sourceId source id
   * @param sourceName name of source
   * @param newConfiguration new configuration
   * @return updated source configuration
   * @throws ConfigNotFoundException thrown if the source does not exist
   * @throws IOException thrown if exception while interacting with the db
   * @throws JsonValidationException thrown if newConfiguration is invalid json
   */
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun partialSource(
    sourceId: UUID,
    sourceName: String?,
    newConfiguration: JsonNode?,
  ): SourceConnection {
    // get existing source
    val persistedSource = sourceService.getSourceConnection(sourceId)
    persistedSource.name = Optional.ofNullable(sourceName).orElse(persistedSource.name)

    // Merge update configuration into the persisted configuration
    val mergeConfiguration = Optional.ofNullable(newConfiguration).orElse(persistedSource.configuration)
    val updatedConfiguration = mergeNodesExceptForSecrets(persistedSource.configuration, mergeConfiguration)

    return Jsons.clone(persistedSource).withConfiguration(updatedConfiguration)
  }

  /**
   * Update the configuration object for a destination.
   *
   * @param destinationId destination id
   * @param destName name of destination
   * @param newConfiguration new configuration
   * @return updated destination configuration
   * @throws ConfigNotFoundException thrown if the destination does not exist
   * @throws IOException thrown if exception while interacting with the db
   * @throws JsonValidationException thrown if newConfiguration is invalid json
   */
  @Throws(
    io.airbyte.config.persistence.ConfigNotFoundException::class,
    IOException::class,
    JsonValidationException::class,
    ConfigNotFoundException::class,
  )
  fun destination(
    destinationId: UUID,
    destName: String?,
    newConfiguration: JsonNode,
  ): DestinationConnection {
    // get existing destination
    val persistedDestination: DestinationConnection
    try {
      persistedDestination = destinationService.getDestinationConnection(destinationId)
    } catch (e: ConfigNotFoundException) {
      throw io.airbyte.config.persistence
        .ConfigNotFoundException(e.type, e.configId)
    }
    persistedDestination.name = destName
    // get spec
    val destinationDefinition =
      destinationService
        .getStandardDestinationDefinition(persistedDestination.destinationDefinitionId)
    val destinationVersion =
      actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, persistedDestination.workspaceId, destinationId)
    val spec = destinationVersion.spec
    // copy any necessary secrets from the current destination to the incoming updated destination
    val updatedConfiguration =
      secretsProcessor.copySecrets(
        persistedDestination.configuration,
        newConfiguration,
        spec.connectionSpecification,
      )

    return Jsons.clone(persistedDestination).withConfiguration(updatedConfiguration)
  }

  /**
   * Partially update the configuration object for a destination.
   *
   * @param destinationId destination id
   * @param destinationName name of destination
   * @param newConfiguration new configuration
   * @return updated destination configuration
   * @throws ConfigNotFoundException thrown if the destination does not exist
   * @throws IOException thrown if exception while interacting with the db
   * @throws JsonValidationException thrown if newConfiguration is invalid json
   */
  @Throws(io.airbyte.config.persistence.ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
  fun partialDestination(
    destinationId: UUID,
    destinationName: String?,
    newConfiguration: JsonNode?,
  ): DestinationConnection {
    // get existing destination
    val persistedDestination: DestinationConnection
    try {
      persistedDestination = destinationService.getDestinationConnection(destinationId)
    } catch (e: ConfigNotFoundException) {
      throw io.airbyte.config.persistence
        .ConfigNotFoundException(e.type, e.configId)
    }
    persistedDestination.name = Optional.ofNullable(destinationName).orElse(persistedDestination.name)

    // Merge update configuration into the persisted configuration
    val mergeConfiguration = Optional.ofNullable(newConfiguration).orElse(persistedDestination.configuration)
    val updatedConfiguration =
      mergeNodesExceptForSecrets(persistedDestination.configuration, mergeConfiguration)

    return Jsons.clone(persistedDestination).withConfiguration(updatedConfiguration)
  }
}
