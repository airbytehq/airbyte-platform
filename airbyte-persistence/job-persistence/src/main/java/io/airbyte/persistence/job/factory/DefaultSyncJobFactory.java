/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.factory;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSyncOperation;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.ConfigInjector;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.domain.StreamRefresh;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.OperationService;
import io.airbyte.data.services.SourceService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.persistence.job.DefaultJobCreator;
import io.airbyte.persistence.job.WorkspaceHelper;
import io.airbyte.persistence.job.helper.model.JobCreatorInput;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

/**
 * Creates a sync job record in the db.
 */
public class DefaultSyncJobFactory implements SyncJobFactory {

  private final boolean connectorSpecificResourceDefaultsEnabled;
  private final DefaultJobCreator jobCreator;
  private final OAuthConfigSupplier oAuthConfigSupplier;
  private final ConfigInjector configInjector;
  private final WorkspaceHelper workspaceHelper;
  private final ActorDefinitionVersionHelper actorDefinitionVersionHelper;

  private final SourceService sourceService;
  private final DestinationService destinationService;
  private final ConnectionService connectionService;
  private final OperationService operationService;
  private final WorkspaceService workspaceService;

  public DefaultSyncJobFactory(final boolean connectorSpecificResourceDefaultsEnabled,
                               final DefaultJobCreator jobCreator,
                               final OAuthConfigSupplier oauthConfigSupplier,
                               final ConfigInjector configInjector,
                               final WorkspaceHelper workspaceHelper,
                               final ActorDefinitionVersionHelper actorDefinitionVersionHelper,
                               final SourceService sourceService,
                               final DestinationService destinationService,
                               final ConnectionService connectionService,
                               final OperationService operationService,
                               final WorkspaceService workspaceService) {
    this.connectorSpecificResourceDefaultsEnabled = connectorSpecificResourceDefaultsEnabled;
    this.jobCreator = jobCreator;
    this.oAuthConfigSupplier = oauthConfigSupplier;
    this.configInjector = configInjector;
    this.workspaceHelper = workspaceHelper;
    this.actorDefinitionVersionHelper = actorDefinitionVersionHelper;
    this.sourceService = sourceService;
    this.destinationService = destinationService;
    this.connectionService = connectionService;
    this.operationService = operationService;
    this.workspaceService = workspaceService;
  }

  @Override
  public Long createSync(final UUID connectionId) {
    try {
      final JobCreatorInput jobCreatorInput = getJobCreatorInput(connectionId);

      return jobCreator.createSyncJob(
          jobCreatorInput.getSource(),
          jobCreatorInput.getDestination(),
          jobCreatorInput.getStandardSync(),
          jobCreatorInput.getSourceDockerImageName(),
          jobCreatorInput.getSourceDockerImageIsDefault(),
          jobCreatorInput.getSourceProtocolVersion(),
          jobCreatorInput.getDestinationDockerImageName(),
          jobCreatorInput.getDestinationDockerImageIsDefault(),
          jobCreatorInput.getDestinationProtocolVersion(),
          jobCreatorInput.getStandardSyncOperations(),
          jobCreatorInput.getWebhookOperationConfigs(),
          jobCreatorInput.getSourceDefinition(),
          jobCreatorInput.getDestinationDefinition(),
          jobCreatorInput.getSourceDefinitionVersion(),
          jobCreatorInput.getDestinationDefinitionVersion(),
          jobCreatorInput.getWorkspaceId())
          .orElseThrow(() -> new IllegalStateException("We shouldn't be trying to create a new sync job if there is one running already."));

    } catch (final IOException | JsonValidationException | ConfigNotFoundException | io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Long createRefresh(UUID connectionId, List<StreamRefresh> streamsToRefresh) {
    try {
      final JobCreatorInput jobCreatorInput = getJobCreatorInput(connectionId);

      return jobCreator.createRefreshConnection(
          jobCreatorInput.getStandardSync(),
          jobCreatorInput.getSourceDockerImageName(),
          jobCreatorInput.getSourceProtocolVersion(),
          jobCreatorInput.getDestinationDockerImageName(),
          jobCreatorInput.getDestinationProtocolVersion(),
          jobCreatorInput.getStandardSyncOperations(),
          jobCreatorInput.getWebhookOperationConfigs(),
          jobCreatorInput.getSourceDefinition(),
          jobCreatorInput.getDestinationDefinition(),
          jobCreatorInput.getSourceDefinitionVersion(),
          jobCreatorInput.getDestinationDefinitionVersion(),
          jobCreatorInput.getWorkspaceId(),
          streamsToRefresh)
          .orElseThrow(() -> new IllegalStateException("We shouldn't be trying to create a new sync job if there is one running already."));

    } catch (final IOException | JsonValidationException | ConfigNotFoundException | io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private JobCreatorInput getJobCreatorInput(UUID connectionId)
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final StandardSync standardSync = connectionService.getStandardSync(connectionId);
    final UUID workspaceId = workspaceHelper.getWorkspaceForSourceId(standardSync.getSourceId());
    final StandardWorkspace workspace = workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true);
    final SourceConnection sourceConnection = sourceService.getSourceConnection(standardSync.getSourceId());
    final DestinationConnection destinationConnection = destinationService.getDestinationConnection(standardSync.getDestinationId());
    final JsonNode sourceConfiguration = oAuthConfigSupplier.injectSourceOAuthParameters(
        sourceConnection.getSourceDefinitionId(),
        sourceConnection.getSourceId(),
        sourceConnection.getWorkspaceId(),
        sourceConnection.getConfiguration());
    sourceConnection.withConfiguration(configInjector.injectConfig(sourceConfiguration, sourceConnection.getSourceDefinitionId()));
    final JsonNode destinationConfiguration = oAuthConfigSupplier.injectDestinationOAuthParameters(
        destinationConnection.getDestinationDefinitionId(),
        destinationConnection.getDestinationId(),
        destinationConnection.getWorkspaceId(),
        destinationConnection.getConfiguration());
    destinationConnection
        .withConfiguration(configInjector.injectConfig(destinationConfiguration, destinationConnection.getDestinationDefinitionId()));
    final StandardSourceDefinition sourceDefinition = sourceService
        .getStandardSourceDefinition(sourceConnection.getSourceDefinitionId());
    final StandardDestinationDefinition destinationDefinition = destinationService
        .getStandardDestinationDefinition(destinationConnection.getDestinationDefinitionId());

    final ActorDefinitionVersion sourceVersion =
        actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, workspaceId, standardSync.getSourceId());
    final ActorDefinitionVersion destinationVersion =
        actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, workspaceId, standardSync.getDestinationId());

    final String sourceImageName = sourceVersion.getDockerRepository() + ":" + sourceVersion.getDockerImageTag();
    final String destinationImageName = destinationVersion.getDockerRepository() + ":" + destinationVersion.getDockerImageTag();

    final ActorDefinitionVersion sourceImageVersionDefault =
        actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, workspaceId);
    final ActorDefinitionVersion destinationImageVersionDefault =
        actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, workspaceId);

    final List<StandardSyncOperation> standardSyncOperations = Lists.newArrayList();
    for (final var operationId : standardSync.getOperationIds()) {
      final StandardSyncOperation standardSyncOperation = operationService.getStandardSyncOperation(operationId);
      standardSyncOperations.add(standardSyncOperation);
    }

    // for OSS users, make it possible to ignore default actor-level resource requirements
    if (!connectorSpecificResourceDefaultsEnabled) {
      sourceDefinition.setResourceRequirements(null);
      destinationDefinition.setResourceRequirements(null);
    }

    return new JobCreatorInput(
        sourceConnection,
        destinationConnection,
        standardSync,
        sourceImageName,
        imageIsDefault(sourceImageName, sourceImageVersionDefault),
        new Version(sourceVersion.getProtocolVersion()),
        destinationImageName,
        imageIsDefault(destinationImageName, destinationImageVersionDefault),
        new Version(destinationVersion.getProtocolVersion()),
        standardSyncOperations,
        workspace.getWebhookOperationConfigs(),
        sourceDefinition,
        destinationDefinition,
        sourceVersion,
        destinationVersion,
        workspaceId);
  }

  Boolean imageIsDefault(final String imageName, final ActorDefinitionVersion imageVersionDefault) {
    if (imageName == null || imageVersionDefault == null) {
      // We assume that if these values are not set there is no override and therefore the version is
      // default
      return true;
    }
    String dockerRepository = imageVersionDefault.getDockerRepository();
    String dockerImageTag = imageVersionDefault.getDockerImageTag();
    if (dockerRepository == null || dockerImageTag == null) {
      return true;
    }
    return imageName.equals(dockerRepository + ":" + dockerImageTag);
  }

}
