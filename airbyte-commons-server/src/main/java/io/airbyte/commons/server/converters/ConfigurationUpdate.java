/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.secrets.JsonSecretsProcessor;
import io.airbyte.config.secrets.SecretsHelpers;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.SourceService;
import io.airbyte.protocol.models.v0.ConnectorSpecification;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

/**
 * Abstraction to manage the updating the configuration of a source or a destination. Helps with
 * secrets handling and make it easy to test these transitions.
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
@Singleton
public class ConfigurationUpdate {

  private final JsonSecretsProcessor secretsProcessor;
  private final ActorDefinitionVersionHelper actorDefinitionVersionHelper;
  private final SourceService sourceService;
  private final DestinationService destinationService;

  public ConfigurationUpdate(final ActorDefinitionVersionHelper actorDefinitionVersionHelper,
                             final SourceService sourceService,
                             final DestinationService destinationService) {
    this(new JsonSecretsProcessor(true), actorDefinitionVersionHelper, sourceService, destinationService);
  }

  public ConfigurationUpdate(final JsonSecretsProcessor secretsProcessor,
                             final ActorDefinitionVersionHelper actorDefinitionVersionHelper,
                             final SourceService sourceService,
                             final DestinationService destinationService) {
    this.secretsProcessor = secretsProcessor;
    this.actorDefinitionVersionHelper = actorDefinitionVersionHelper;
    this.sourceService = sourceService;
    this.destinationService = destinationService;
  }

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
  @SuppressWarnings("PMD.PreserveStackTrace")
  public SourceConnection source(final UUID sourceId, final String sourceName, final JsonNode newConfiguration)
      throws ConfigNotFoundException, IOException, JsonValidationException, io.airbyte.data.ConfigNotFoundException {
    // get existing source
    final SourceConnection persistedSource;
    try {
      persistedSource = sourceService.getSourceConnection(sourceId);
    } catch (final io.airbyte.data.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.type, e.configId);
    }
    persistedSource.setName(sourceName);
    // get spec
    final StandardSourceDefinition sourceDefinition = sourceService.getStandardSourceDefinition(persistedSource.getSourceDefinitionId());
    final ActorDefinitionVersion sourceVersion =
        actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, persistedSource.getWorkspaceId(), sourceId);
    final ConnectorSpecification spec = sourceVersion.getSpec();
    // copy any necessary secrets from the current source to the incoming updated source
    final JsonNode updatedConfiguration = secretsProcessor.copySecrets(
        persistedSource.getConfiguration(),
        newConfiguration,
        spec.getConnectionSpecification());

    return Jsons.clone(persistedSource).withConfiguration(updatedConfiguration);
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
  @SuppressWarnings("PMD.PreserveStackTrace")
  public SourceConnection partialSource(final UUID sourceId, final String sourceName, final JsonNode newConfiguration)
      throws IOException, JsonValidationException, io.airbyte.data.ConfigNotFoundException {
    // get existing source
    final SourceConnection persistedSource = sourceService.getSourceConnection(sourceId);
    persistedSource.setName(Optional.ofNullable(sourceName).orElse(persistedSource.getName()));

    // Merge update configuration into the persisted configuration
    final JsonNode mergeConfiguration = Optional.ofNullable(newConfiguration).orElse(persistedSource.getConfiguration());
    final JsonNode updatedConfiguration = SecretsHelpers.INSTANCE.mergeNodesExceptForSecrets(persistedSource.getConfiguration(), mergeConfiguration);

    return Jsons.clone(persistedSource).withConfiguration(updatedConfiguration);
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
  @SuppressWarnings("PMD.PreserveStackTrace")
  public DestinationConnection destination(final UUID destinationId, final String destName, final JsonNode newConfiguration)
      throws ConfigNotFoundException, IOException, JsonValidationException, io.airbyte.data.ConfigNotFoundException {
    // get existing destination
    final DestinationConnection persistedDestination;
    try {
      persistedDestination = destinationService.getDestinationConnection(destinationId);
    } catch (final io.airbyte.data.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.type, e.configId);
    }
    persistedDestination.setName(destName);
    // get spec
    final StandardDestinationDefinition destinationDefinition = destinationService
        .getStandardDestinationDefinition(persistedDestination.getDestinationDefinitionId());
    final ActorDefinitionVersion destinationVersion =
        actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, persistedDestination.getWorkspaceId(), destinationId);
    final ConnectorSpecification spec = destinationVersion.getSpec();
    // copy any necessary secrets from the current destination to the incoming updated destination
    final JsonNode updatedConfiguration = secretsProcessor.copySecrets(
        persistedDestination.getConfiguration(),
        newConfiguration,
        spec.getConnectionSpecification());

    return Jsons.clone(persistedDestination).withConfiguration(updatedConfiguration);
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
  @SuppressWarnings("PMD.PreserveStackTrace")
  public DestinationConnection partialDestination(final UUID destinationId, final String destinationName, final JsonNode newConfiguration)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    // get existing destination
    final DestinationConnection persistedDestination;
    try {
      persistedDestination = destinationService.getDestinationConnection(destinationId);
    } catch (final io.airbyte.data.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.type, e.configId);
    }
    persistedDestination.setName(Optional.ofNullable(destinationName).orElse(persistedDestination.getName()));

    // Merge update configuration into the persisted configuration
    final JsonNode mergeConfiguration = Optional.ofNullable(newConfiguration).orElse(persistedDestination.getConfiguration());
    final JsonNode updatedConfiguration =
        SecretsHelpers.INSTANCE.mergeNodesExceptForSecrets(persistedDestination.getConfiguration(), mergeConfiguration);

    return Jsons.clone(persistedDestination).withConfiguration(updatedConfiguration);
  }

}
