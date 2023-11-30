/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import io.airbyte.api.model.generated.ConnectionRead;
import io.airbyte.api.model.generated.DestinationCloneConfiguration;
import io.airbyte.api.model.generated.DestinationCloneRequestBody;
import io.airbyte.api.model.generated.DestinationCreate;
import io.airbyte.api.model.generated.DestinationDefinitionIdRequestBody;
import io.airbyte.api.model.generated.DestinationIdRequestBody;
import io.airbyte.api.model.generated.DestinationRead;
import io.airbyte.api.model.generated.DestinationReadList;
import io.airbyte.api.model.generated.DestinationSearch;
import io.airbyte.api.model.generated.DestinationSnippetRead;
import io.airbyte.api.model.generated.DestinationUpdate;
import io.airbyte.api.model.generated.ListResourcesForWorkspacesRequestBody;
import io.airbyte.api.model.generated.PartialDestinationUpdate;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.converters.ConfigurationUpdate;
import io.airbyte.commons.server.handlers.helpers.OAuthSecretHelper;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.config.persistence.ConfigRepository.ResourcesQueryPaginated;
import io.airbyte.config.secrets.JsonSecretsProcessor;
import io.airbyte.data.services.DestinationService;
import io.airbyte.persistence.job.factory.OAuthConfigSupplier;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.validation.json.JsonSchemaValidator;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * DestinationHandler. Javadocs suppressed because api docs should be used as source of truth.
 */
@SuppressWarnings("ParameterName")
@Singleton
public class DestinationHandler {

  private final ConnectionsHandler connectionsHandler;
  private final Supplier<UUID> uuidGenerator;
  private final ConfigRepository configRepository;
  private final JsonSchemaValidator validator;
  private final ConfigurationUpdate configurationUpdate;
  private final JsonSecretsProcessor secretsProcessor;
  private final OAuthConfigSupplier oAuthConfigSupplier;
  private final ActorDefinitionVersionHelper actorDefinitionVersionHelper;
  private final DestinationService destinationService;

  @VisibleForTesting
  public DestinationHandler(final ConfigRepository configRepository,
                            final JsonSchemaValidator integrationSchemaValidation,
                            final ConnectionsHandler connectionsHandler,
                            @Named("uuidGenerator") final Supplier<UUID> uuidGenerator,
                            @Named("jsonSecretsProcessorWithCopy") final JsonSecretsProcessor secretsProcessor,
                            final ConfigurationUpdate configurationUpdate,
                            final OAuthConfigSupplier oAuthConfigSupplier,
                            final ActorDefinitionVersionHelper actorDefinitionVersionHelper,
                            final DestinationService destinationService) {
    this.configRepository = configRepository;
    this.validator = integrationSchemaValidation;
    this.connectionsHandler = connectionsHandler;
    this.uuidGenerator = uuidGenerator;
    this.configurationUpdate = configurationUpdate;
    this.secretsProcessor = secretsProcessor;
    this.oAuthConfigSupplier = oAuthConfigSupplier;
    this.actorDefinitionVersionHelper = actorDefinitionVersionHelper;
    this.destinationService = destinationService;
  }

  public DestinationRead createDestination(final DestinationCreate destinationCreate)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    // validate configuration
    final ConnectorSpecification spec = getSpecForWorkspaceId(destinationCreate.getDestinationDefinitionId(), destinationCreate.getWorkspaceId());
    validateDestination(spec, destinationCreate.getConnectionConfiguration());

    // persist
    final UUID destinationId = uuidGenerator.get();
    persistDestinationConnection(
        destinationCreate.getName() != null ? destinationCreate.getName() : "default",
        destinationCreate.getDestinationDefinitionId(),
        destinationCreate.getWorkspaceId(),
        destinationId,
        destinationCreate.getConnectionConfiguration(),
        false,
        spec);

    // read configuration from db
    return buildDestinationRead(configRepository.getDestinationConnection(destinationId), spec);
  }

  public void deleteDestination(final DestinationIdRequestBody destinationIdRequestBody)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    // get existing implementation
    final DestinationRead destination = buildDestinationRead(destinationIdRequestBody.getDestinationId());

    deleteDestination(destination);
  }

  @SuppressWarnings("PMD.PreserveStackTrace")
  public void deleteDestination(final DestinationRead destination)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    // disable all connections associated with this destination
    // Delete connections first in case it fails in the middle, destination will still be visible
    final WorkspaceIdRequestBody workspaceIdRequestBody = new WorkspaceIdRequestBody().workspaceId(destination.getWorkspaceId());
    for (final ConnectionRead connectionRead : connectionsHandler.listConnectionsForWorkspace(workspaceIdRequestBody).getConnections()) {
      if (!connectionRead.getDestinationId().equals(destination.getDestinationId())) {
        continue;
      }

      connectionsHandler.deleteConnection(connectionRead.getConnectionId());
    }

    final JsonNode fullConfig;
    try {
      fullConfig = destinationService.getDestinationConnectionWithSecrets(destination.getDestinationId()).getConfiguration();
    } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.getType(), e.getConfigId());
    }
    final ConnectorSpecification spec =
        getSpecForDestinationId(destination.getDestinationDefinitionId(), destination.getWorkspaceId(), destination.getDestinationId());

    // persist
    persistDestinationConnection(
        destination.getName(),
        destination.getDestinationDefinitionId(),
        destination.getWorkspaceId(),
        destination.getDestinationId(),
        fullConfig,
        true,
        spec);
  }

  public DestinationRead updateDestination(final DestinationUpdate destinationUpdate)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    // get existing implementation
    final DestinationConnection updatedDestination = configurationUpdate
        .destination(destinationUpdate.getDestinationId(), destinationUpdate.getName(), destinationUpdate.getConnectionConfiguration());

    final ConnectorSpecification spec =
        getSpecForDestinationId(updatedDestination.getDestinationDefinitionId(), updatedDestination.getWorkspaceId(),
            updatedDestination.getDestinationId());

    // validate configuration
    validateDestination(spec, updatedDestination.getConfiguration());

    // persist
    persistDestinationConnection(
        updatedDestination.getName(),
        updatedDestination.getDestinationDefinitionId(),
        updatedDestination.getWorkspaceId(),
        updatedDestination.getDestinationId(),
        updatedDestination.getConfiguration(),
        updatedDestination.getTombstone(),
        spec);

    // read configuration from db
    return buildDestinationRead(
        configRepository.getDestinationConnection(destinationUpdate.getDestinationId()), spec);
  }

  public DestinationRead partialDestinationUpdate(final PartialDestinationUpdate partialDestinationUpdate)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    // get existing implementation
    final DestinationConnection updatedDestination = configurationUpdate
        .partialDestination(partialDestinationUpdate.getDestinationId(), partialDestinationUpdate.getName(),
            partialDestinationUpdate.getConnectionConfiguration());

    final ConnectorSpecification spec =
        getSpecForDestinationId(updatedDestination.getDestinationDefinitionId(), updatedDestination.getWorkspaceId(),
            updatedDestination.getDestinationId());

    OAuthSecretHelper.validateNoSecretsInConfiguration(spec, partialDestinationUpdate.getConnectionConfiguration());

    // validate configuration
    validateDestination(spec, updatedDestination.getConfiguration());

    // persist
    persistDestinationConnection(
        updatedDestination.getName(),
        updatedDestination.getDestinationDefinitionId(),
        updatedDestination.getWorkspaceId(),
        updatedDestination.getDestinationId(),
        updatedDestination.getConfiguration(),
        updatedDestination.getTombstone(),
        spec);

    // read configuration from db
    return buildDestinationRead(
        configRepository.getDestinationConnection(partialDestinationUpdate.getDestinationId()), spec);
  }

  /**
   * Upgrades the destination to the destination definition's default version.
   *
   * @param destinationIdRequestBody - ID of the destination to upgrade
   */
  public void upgradeDestinationVersion(final DestinationIdRequestBody destinationIdRequestBody)
      throws IOException, JsonValidationException, ConfigNotFoundException {
    final DestinationConnection destinationConnection = configRepository.getDestinationConnection(destinationIdRequestBody.getDestinationId());
    final StandardDestinationDefinition destinationDefinition =
        configRepository.getStandardDestinationDefinition(destinationConnection.getDestinationDefinitionId());
    configRepository.setActorDefaultVersion(destinationIdRequestBody.getDestinationId(), destinationDefinition.getDefaultVersionId());
  }

  public DestinationRead getDestination(final DestinationIdRequestBody destinationIdRequestBody)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    return buildDestinationRead(destinationIdRequestBody.getDestinationId());
  }

  public DestinationRead cloneDestination(final DestinationCloneRequestBody destinationCloneRequestBody)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    // read destination configuration from db
    final DestinationRead destinationToClone = buildDestinationReadWithSecrets(destinationCloneRequestBody.getDestinationCloneId());
    final DestinationCloneConfiguration destinationCloneConfiguration = destinationCloneRequestBody.getDestinationConfiguration();

    final String copyText = " (Copy)";
    final String destinationName = destinationToClone.getName() + copyText;

    final DestinationCreate destinationCreate = new DestinationCreate()
        .name(destinationName)
        .destinationDefinitionId(destinationToClone.getDestinationDefinitionId())
        .connectionConfiguration(destinationToClone.getConnectionConfiguration())
        .workspaceId(destinationToClone.getWorkspaceId());

    if (destinationCloneConfiguration != null) {
      if (destinationCloneConfiguration.getName() != null) {
        destinationCreate.name(destinationCloneConfiguration.getName());
      }

      if (destinationCloneConfiguration.getConnectionConfiguration() != null) {
        destinationCreate.connectionConfiguration(destinationCloneConfiguration.getConnectionConfiguration());
      }
    }

    return createDestination(destinationCreate);
  }

  public DestinationReadList listDestinationsForWorkspace(final WorkspaceIdRequestBody workspaceIdRequestBody)
      throws ConfigNotFoundException, IOException, JsonValidationException {

    final List<DestinationRead> destinationReads = new ArrayList<>();
    final List<DestinationConnection> destinationConnections =
        configRepository.listWorkspaceDestinationConnection(workspaceIdRequestBody.getWorkspaceId());
    for (final DestinationConnection destinationConnection : destinationConnections) {
      destinationReads.add(buildDestinationRead(destinationConnection));
    }

    return new DestinationReadList().destinations(destinationReads);
  }

  public DestinationReadList listDestinationsForWorkspaces(final ListResourcesForWorkspacesRequestBody listResourcesForWorkspacesRequestBody)
      throws ConfigNotFoundException, IOException, JsonValidationException {

    final List<DestinationRead> reads = Lists.newArrayList();
    final List<DestinationConnection> destinationConnections = configRepository.listWorkspacesDestinationConnections(
        new ResourcesQueryPaginated(
            listResourcesForWorkspacesRequestBody.getWorkspaceIds(),
            listResourcesForWorkspacesRequestBody.getIncludeDeleted(),
            listResourcesForWorkspacesRequestBody.getPagination().getPageSize(),
            listResourcesForWorkspacesRequestBody.getPagination().getRowOffset(), null));
    for (final DestinationConnection destinationConnection : destinationConnections) {
      reads.add(buildDestinationRead(destinationConnection));
    }
    return new DestinationReadList().destinations(reads);
  }

  public DestinationReadList listDestinationsForDestinationDefinition(final DestinationDefinitionIdRequestBody destinationDefinitionIdRequestBody)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    final List<DestinationRead> reads = Lists.newArrayList();

    for (final DestinationConnection destinationConnection : configRepository
        .listDestinationsForDefinition(destinationDefinitionIdRequestBody.getDestinationDefinitionId())) {
      reads.add(buildDestinationRead(destinationConnection));
    }

    return new DestinationReadList().destinations(reads);
  }

  public DestinationReadList searchDestinations(final DestinationSearch destinationSearch)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final List<DestinationRead> reads = Lists.newArrayList();

    for (final DestinationConnection dci : configRepository.listDestinationConnection()) {
      if (!dci.getTombstone()) {
        final DestinationRead destinationRead = buildDestinationRead(dci);
        if (connectionsHandler.matchSearch(destinationSearch, destinationRead)) {
          reads.add(destinationRead);
        }
      }
    }

    return new DestinationReadList().destinations(reads);
  }

  private void validateDestination(final ConnectorSpecification spec, final JsonNode configuration) throws JsonValidationException {
    validator.ensure(spec.getConnectionSpecification(), configuration);
  }

  public ConnectorSpecification getSpecForDestinationId(final UUID destinationDefinitionId, final UUID workspaceId, final UUID destinationId)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    final StandardDestinationDefinition destinationDefinition = configRepository.getStandardDestinationDefinition(destinationDefinitionId);
    final ActorDefinitionVersion destinationVersion =
        actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, workspaceId, destinationId);
    return destinationVersion.getSpec();
  }

  public ConnectorSpecification getSpecForWorkspaceId(final UUID destinationDefinitionId, final UUID workspaceId)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    final StandardDestinationDefinition destinationDefinition = configRepository.getStandardDestinationDefinition(destinationDefinitionId);
    final ActorDefinitionVersion destinationVersion = actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, workspaceId);
    return destinationVersion.getSpec();
  }

  @SuppressWarnings("PMD.PreserveStackTrace")
  private void persistDestinationConnection(final String name,
                                            final UUID destinationDefinitionId,
                                            final UUID workspaceId,
                                            final UUID destinationId,
                                            final JsonNode configurationJson,
                                            final boolean tombstone,
                                            final ConnectorSpecification spec)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    final JsonNode oAuthMaskedConfigurationJson =
        oAuthConfigSupplier.maskDestinationOAuthParameters(destinationDefinitionId, workspaceId, configurationJson, spec);
    final DestinationConnection destinationConnection = new DestinationConnection()
        .withName(name)
        .withDestinationDefinitionId(destinationDefinitionId)
        .withWorkspaceId(workspaceId)
        .withDestinationId(destinationId)
        .withConfiguration(oAuthMaskedConfigurationJson)
        .withTombstone(tombstone);
    try {
      destinationService.writeDestinationConnectionWithSecrets(destinationConnection, spec);
    } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.getType(), e.getConfigId());
    }
  }

  private DestinationRead buildDestinationRead(final UUID destinationId) throws JsonValidationException, IOException, ConfigNotFoundException {
    return buildDestinationRead(configRepository.getDestinationConnection(destinationId));
  }

  private DestinationRead buildDestinationRead(final DestinationConnection destinationConnection)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    final ConnectorSpecification spec =
        getSpecForDestinationId(destinationConnection.getDestinationDefinitionId(), destinationConnection.getWorkspaceId(),
            destinationConnection.getDestinationId());
    return buildDestinationRead(destinationConnection, spec);
  }

  private DestinationRead buildDestinationRead(final DestinationConnection destinationConnection, final ConnectorSpecification spec)
      throws ConfigNotFoundException, IOException, JsonValidationException {

    // remove secrets from config before returning the read
    final DestinationConnection dci = Jsons.clone(destinationConnection);
    dci.setConfiguration(secretsProcessor.prepareSecretsForOutput(dci.getConfiguration(), spec.getConnectionSpecification()));

    final StandardDestinationDefinition standardDestinationDefinition =
        configRepository.getStandardDestinationDefinition(dci.getDestinationDefinitionId());
    return toDestinationRead(dci, standardDestinationDefinition);
  }

  @SuppressWarnings("PMD.PreserveStackTrace")
  private DestinationRead buildDestinationReadWithSecrets(final UUID destinationId)
      throws ConfigNotFoundException, IOException, JsonValidationException {

    // remove secrets from config before returning the read
    final DestinationConnection dci;
    try {
      dci = Jsons.clone(destinationService.getDestinationConnectionWithSecrets(destinationId));
    } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.getType(), e.getConfigId());
    }
    final StandardDestinationDefinition standardDestinationDefinition =
        configRepository.getStandardDestinationDefinition(dci.getDestinationDefinitionId());
    return toDestinationRead(dci, standardDestinationDefinition);
  }

  protected static DestinationRead toDestinationRead(final DestinationConnection destinationConnection,
                                                     final StandardDestinationDefinition standardDestinationDefinition) {
    return new DestinationRead()
        .destinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .destinationId(destinationConnection.getDestinationId())
        .workspaceId(destinationConnection.getWorkspaceId())
        .destinationDefinitionId(destinationConnection.getDestinationDefinitionId())
        .connectionConfiguration(destinationConnection.getConfiguration())
        .name(destinationConnection.getName())
        .destinationName(standardDestinationDefinition.getName())
        .icon(DestinationDefinitionsHandler.loadIcon(standardDestinationDefinition.getIcon()));
  }

  protected static DestinationSnippetRead toDestinationSnippetRead(final DestinationConnection destinationConnection,
                                                                   final StandardDestinationDefinition standardDestinationDefinition) {
    return new DestinationSnippetRead()
        .destinationId(destinationConnection.getDestinationId())
        .name(destinationConnection.getName())
        .destinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .destinationName(standardDestinationDefinition.getName())
        .icon(DestinationDefinitionsHandler.loadIcon(standardDestinationDefinition.getIcon()));
  }

}
