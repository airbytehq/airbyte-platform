/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
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
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.config.specs.RemoteDefinitionsProvider;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * ConnectorDocumentationHandler. Javadocs suppressed because api docs should be used as source of
 * truth.
 */
@SuppressWarnings({"MissingJavadocMethod"})
@Singleton
public class ConnectorDocumentationHandler {

  static final String LATEST = "latest";

  private final ConfigRepository configRepository;
  private final ActorDefinitionVersionHelper actorDefinitionVersionHelper;
  private final RemoteDefinitionsProvider remoteDefinitionsProvider;

  public ConnectorDocumentationHandler(final ConfigRepository configRepository,
                                       final ActorDefinitionVersionHelper actorDefinitionVersionHelper,
                                       final RemoteDefinitionsProvider remoteDefinitionsProvider) {
    this.configRepository = configRepository;
    this.actorDefinitionVersionHelper = actorDefinitionVersionHelper;
    this.remoteDefinitionsProvider = remoteDefinitionsProvider;
  }

  public ConnectorDocumentationRead getConnectorDocumentation(final ConnectorDocumentationRequestBody request)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final ActorDefinitionVersion actorDefinitionVersion = request.getActorType().equals(ActorType.SOURCE)
        ? getSourceActorDefinitionVersion(request.getActorDefinitionId(), request.getWorkspaceId(), request.getActorId())
        : getDestinationActorDefinitionVersion(request.getActorDefinitionId(), request.getWorkspaceId(), request.getActorId());
    final String dockerRepo = actorDefinitionVersion.getDockerRepository();
    final String version = actorDefinitionVersion.getDockerImageTag();

    // prioritize versioned over latest, then inapp over full
    final Optional<String> versionedInappDocString = remoteDefinitionsProvider.getConnectorDocumentation(dockerRepo, version, true);
    if (versionedInappDocString.isPresent()) {
      return new ConnectorDocumentationRead().doc(versionedInappDocString.get());
    }

    final Optional<String> versionedFullDocString = remoteDefinitionsProvider.getConnectorDocumentation(dockerRepo, version, false);
    if (versionedFullDocString.isPresent()) {
      return new ConnectorDocumentationRead().doc(versionedFullDocString.get());
    }

    final Optional<String> latestInappDocString = remoteDefinitionsProvider.getConnectorDocumentation(dockerRepo, LATEST, true);
    if (latestInappDocString.isPresent()) {
      return new ConnectorDocumentationRead().doc(latestInappDocString.get());
    }

    final Optional<String> latestFullDocString = remoteDefinitionsProvider.getConnectorDocumentation(dockerRepo, LATEST, false);
    if (latestFullDocString.isPresent()) {
      return new ConnectorDocumentationRead().doc(latestFullDocString.get());
    }

    throw new NotFoundException(String.format("Could not find any documentation for connector %s", dockerRepo));
  }

  private ActorDefinitionVersion getSourceActorDefinitionVersion(final UUID sourceDefinitionId, final UUID workspaceId, @Nullable final UUID sourceId)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final StandardSourceDefinition sourceDefinition = configRepository.getStandardSourceDefinition(sourceDefinitionId);
    return actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, workspaceId, sourceId);
  }

  private ActorDefinitionVersion getDestinationActorDefinitionVersion(final UUID destDefinitionId,
                                                                      final UUID workspaceId,
                                                                      @Nullable final UUID destId)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final StandardDestinationDefinition destDefinition = configRepository.getStandardDestinationDefinition(destDefinitionId);
    return actorDefinitionVersionHelper.getDestinationVersion(destDefinition, workspaceId, destId);
  }

}
