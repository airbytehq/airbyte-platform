/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import io.airbyte.api.model.generated.ActorType;
import io.airbyte.api.model.generated.ConnectorDocumentationRead;
import io.airbyte.api.model.generated.ConnectorDocumentationRequestBody;
import io.airbyte.commons.server.errors.NotFoundException;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.specs.RemoteDefinitionsProvider;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.SourceService;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

/**
 * ConnectorDocumentationHandler. Javadocs suppressed because api docs should be used as source of
 * truth.
 */
@SuppressWarnings({"MissingJavadocMethod"})
@Singleton
public class ConnectorDocumentationHandler {

  static final String LATEST = "latest";

  private final ActorDefinitionVersionHelper actorDefinitionVersionHelper;
  private final RemoteDefinitionsProvider remoteDefinitionsProvider;

  private final SourceService sourceService;
  private final DestinationService destinationService;

  public ConnectorDocumentationHandler(final ActorDefinitionVersionHelper actorDefinitionVersionHelper,
                                       final RemoteDefinitionsProvider remoteDefinitionsProvider,
                                       final SourceService sourceService,
                                       final DestinationService destinationService) {
    this.actorDefinitionVersionHelper = actorDefinitionVersionHelper;
    this.remoteDefinitionsProvider = remoteDefinitionsProvider;
    this.sourceService = sourceService;
    this.destinationService = destinationService;
  }

  public ConnectorDocumentationRead getConnectorDocumentation(final ConnectorDocumentationRequestBody request)
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final ActorDefinitionVersion actorDefinitionVersion = request.getActorType().equals(ActorType.SOURCE)
        ? getSourceActorDefinitionVersion(request.getActorDefinitionId(), request.getWorkspaceId(), request.getActorId())
        : getDestinationActorDefinitionVersion(request.getActorDefinitionId(), request.getWorkspaceId(), request.getActorId());
    final String dockerRepo = actorDefinitionVersion.getDockerRepository();
    final String version = actorDefinitionVersion.getDockerImageTag();

    // prioritize versioned over latest
    final Optional<String> versionedDocString = remoteDefinitionsProvider.getConnectorDocumentation(dockerRepo, version);
    if (versionedDocString.isPresent()) {
      return new ConnectorDocumentationRead().doc(versionedDocString.get());
    }

    final Optional<String> latestDocString = remoteDefinitionsProvider.getConnectorDocumentation(dockerRepo, LATEST);
    if (latestDocString.isPresent()) {
      return new ConnectorDocumentationRead().doc(latestDocString.get());
    }

    throw new NotFoundException(String.format("Could not find any documentation for connector %s", dockerRepo));
  }

  private ActorDefinitionVersion getSourceActorDefinitionVersion(final UUID sourceDefinitionId, final UUID workspaceId, @Nullable final UUID sourceId)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final StandardSourceDefinition sourceDefinition = sourceService.getStandardSourceDefinition(sourceDefinitionId);
    return actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, workspaceId, sourceId);
  }

  private ActorDefinitionVersion getDestinationActorDefinitionVersion(final UUID destDefinitionId,
                                                                      final UUID workspaceId,
                                                                      @Nullable final UUID destId)
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final StandardDestinationDefinition destDefinition = destinationService.getStandardDestinationDefinition(destDefinitionId);
    return actorDefinitionVersionHelper.getDestinationVersion(destDefinition, workspaceId, destId);
  }

}
