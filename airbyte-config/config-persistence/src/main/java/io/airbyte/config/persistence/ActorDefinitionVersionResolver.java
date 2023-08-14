/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.ConnectorRegistryDestinationDefinition;
import io.airbyte.config.ConnectorRegistrySourceDefinition;
import io.airbyte.config.helpers.ConnectorRegistryConverters;
import io.airbyte.config.specs.RemoteDefinitionsProvider;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for resolving ActorDefinitionVersions based on their existence locally in the
 * database or remotely in the DefinitionsProvider.
 */
@Singleton
@Requires(bean = ConfigRepository.class)
public class ActorDefinitionVersionResolver {

  private final RemoteDefinitionsProvider remoteDefinitionsProvider;
  private final ConfigRepository configRepository;
  private static final Logger LOGGER = LoggerFactory.getLogger(ActorDefinitionVersionResolver.class);

  public ActorDefinitionVersionResolver(final RemoteDefinitionsProvider remoteDefinitionsProvider,
                                        final ConfigRepository configRepository) {
    this.remoteDefinitionsProvider = remoteDefinitionsProvider;
    this.configRepository = configRepository;
  }

  /**
   * Resolves an ActorDefinitionVersion for a given actor definition id and docker image tag. If the
   * ADV does not already exist in the database, it will attempt to fetch the associated registry
   * definition from the DefinitionsProvider and use it to write a new ADV to the database.
   *
   * @return ActorDefinitionVersion if the version was resolved, otherwise empty optional
   */
  public Optional<ActorDefinitionVersion> resolveVersionForTag(final UUID actorDefinitionId,
                                                               final ActorType actorType,
                                                               final String dockerRepository,
                                                               final String dockerImageTag)
      throws IOException {

    final Optional<ActorDefinitionVersion> existingVersion =
        configRepository.getActorDefinitionVersion(actorDefinitionId, dockerImageTag);
    if (existingVersion.isPresent()) {
      return existingVersion;
    }

    final Optional<ActorDefinitionVersion> registryDefinitionVersion =
        fetchRemoteActorDefinitionVersion(actorType, dockerRepository, dockerImageTag);
    if (registryDefinitionVersion.isEmpty()) {
      return Optional.empty();
    }

    final ActorDefinitionVersion newVersion = registryDefinitionVersion.get().withActorDefinitionId(actorDefinitionId);
    final ActorDefinitionVersion persistedADV = configRepository.writeActorDefinitionVersion(newVersion);
    LOGGER.info("Persisted new version {} for definition {} with tag {}", persistedADV.getVersionId(), actorDefinitionId, dockerImageTag);

    return Optional.of(persistedADV);

  }

  private Optional<ActorDefinitionVersion> fetchRemoteActorDefinitionVersion(final ActorType actorType,
                                                                             final String connectorRepository,
                                                                             final String dockerImageTag) {
    final Optional<ActorDefinitionVersion> actorDefinitionVersion;
    switch (actorType) {
      case SOURCE -> {
        final Optional<ConnectorRegistrySourceDefinition> registryDef =
            remoteDefinitionsProvider.getSourceDefinitionByVersion(connectorRepository, dockerImageTag);
        actorDefinitionVersion = registryDef.map(ConnectorRegistryConverters::toActorDefinitionVersion);
      }
      case DESTINATION -> {
        final Optional<ConnectorRegistryDestinationDefinition> registryDef =
            remoteDefinitionsProvider.getDestinationDefinitionByVersion(connectorRepository, dockerImageTag);
        actorDefinitionVersion = registryDef.map(ConnectorRegistryConverters::toActorDefinitionVersion);
      }
      default -> throw new IllegalArgumentException("Actor type not supported: " + actorType);
    }

    if (actorDefinitionVersion.isEmpty()) {
      LOGGER.error("Failed to fetch registry entry for {}:{}", connectorRepository, dockerImageTag);
      return Optional.empty();
    }

    LOGGER.info("Fetched registry entry for {}:{}.", connectorRepository, dockerImageTag);
    return actorDefinitionVersion;
  }

}
