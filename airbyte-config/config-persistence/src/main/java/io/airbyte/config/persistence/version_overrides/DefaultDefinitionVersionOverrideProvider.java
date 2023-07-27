/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence.version_overrides;

import io.airbyte.commons.version.AirbyteProtocolVersionRange;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.ConnectorRegistryDestinationDefinition;
import io.airbyte.config.ConnectorRegistrySourceDefinition;
import io.airbyte.config.helpers.ConnectorRegistryConverters;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.config.specs.RemoteDefinitionsProvider;
import io.airbyte.featureflag.ConnectorVersionOverride;
import io.airbyte.featureflag.Context;
import io.airbyte.featureflag.Destination;
import io.airbyte.featureflag.DestinationDefinition;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.Multi;
import io.airbyte.featureflag.Source;
import io.airbyte.featureflag.SourceDefinition;
import io.airbyte.featureflag.Workspace;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.util.StringUtils;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link DefinitionVersionOverrideProvider} that looks up connector version
 * overrides from the Feature Flag client.
 */
@Singleton
public class DefaultDefinitionVersionOverrideProvider implements DefinitionVersionOverrideProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultDefinitionVersionOverrideProvider.class);

  private final ConfigRepository configRepository;
  private final RemoteDefinitionsProvider remoteDefinitionsProvider;
  private final FeatureFlagClient featureFlagClient;
  private final AirbyteProtocolVersionRange protocolVersionRange;

  public DefaultDefinitionVersionOverrideProvider(final ConfigRepository configRepository,
                                                  final RemoteDefinitionsProvider remoteDefinitionsProvider,
                                                  final FeatureFlagClient featureFlagClient,
                                                  final AirbyteProtocolVersionRange protocolVersionRange) {
    this.configRepository = configRepository;
    this.featureFlagClient = featureFlagClient;
    this.protocolVersionRange = protocolVersionRange;
    this.remoteDefinitionsProvider = remoteDefinitionsProvider;
    LOGGER.info("Initialized feature flag definition version overrides");
  }

  /**
   * Returns the contexts that should be passed in the Multi context when evaluating the version
   * overrides feature flag.
   */
  public List<Context> getContexts(final ActorType actorType, final UUID actorDefinitionId, final UUID workspaceId, @Nullable final UUID actorId) {
    final ArrayList<Context> contexts = new ArrayList<>();

    contexts.add(new Workspace(workspaceId));

    switch (actorType) {
      case SOURCE -> {
        contexts.add(new SourceDefinition(actorDefinitionId));

        if (actorId != null) {
          contexts.add(new Source(actorId));
        }
      }
      case DESTINATION -> {
        contexts.add(new DestinationDefinition(actorDefinitionId));

        if (actorId != null) {
          contexts.add(new Destination(actorId));
        }
      }
      default -> throw new IllegalArgumentException("Actor type not supported: " + actorType);
    }

    return contexts;
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
      LOGGER.error("Failed to fetch registry entry for version override {}:{}", connectorRepository, dockerImageTag);
      return Optional.empty();
    }

    LOGGER.info("Fetched registry entry for {}:{}.", connectorRepository, dockerImageTag);
    return actorDefinitionVersion;
  }

  /**
   * Resolves an ActorDefinitionVersion from the database for a given docker image tag. If the version
   * is not found in the database, it will attempt to fetch the registry entry from the
   * DefinitionsProvider and create a new ADV.
   *
   * @return ActorDefinitionVersion if the version was resolved, otherwise empty optional
   */
  private Optional<ActorDefinitionVersion> resolveVersionForTag(final UUID actorDefinitionId,
                                                                final ActorType actorType,
                                                                final String dockerImageTag,
                                                                final ActorDefinitionVersion defaultVersion) {
    if (StringUtils.isEmpty(dockerImageTag)) {
      return Optional.empty();
    }

    try {
      final Optional<ActorDefinitionVersion> existingVersion =
          configRepository.getActorDefinitionVersion(actorDefinitionId, dockerImageTag);
      if (existingVersion.isPresent()) {
        return existingVersion;
      }

      final Optional<ActorDefinitionVersion> registryDefinitionVersion =
          fetchRemoteActorDefinitionVersion(actorType, defaultVersion.getDockerRepository(), dockerImageTag);
      if (registryDefinitionVersion.isEmpty()) {
        return Optional.empty();
      }

      final ActorDefinitionVersion newVersion = registryDefinitionVersion.get().withActorDefinitionId(actorDefinitionId);
      final ActorDefinitionVersion persistedADV = configRepository.writeActorDefinitionVersion(newVersion);
      LOGGER.info("Persisted new version {} for definition {} with tag {}", persistedADV.getVersionId(), actorDefinitionId, dockerImageTag);

      return Optional.of(persistedADV);
    } catch (final IOException e) {
      LOGGER.error("Failed to read or persist override for definition {} with tag {}", actorDefinitionId, dockerImageTag, e);
      return Optional.empty();
    }

  }

  @Override
  public Optional<ActorDefinitionVersion> getOverride(final ActorType actorType,
                                                      final UUID actorDefinitionId,
                                                      final UUID workspaceId,
                                                      @Nullable final UUID actorId,
                                                      final ActorDefinitionVersion defaultVersion) {
    final List<Context> contexts = getContexts(actorType, actorDefinitionId, workspaceId, actorId);
    final String overrideTag = featureFlagClient.stringVariation(ConnectorVersionOverride.INSTANCE, new Multi(contexts));
    final Optional<ActorDefinitionVersion> version = resolveVersionForTag(actorDefinitionId, actorType, overrideTag, defaultVersion);
    if (version.isPresent()) {
      final Version protocolVersion = new Version(version.get().getProtocolVersion());
      if (!protocolVersionRange.isSupported(protocolVersion)) {
        throw new RuntimeException(String.format(
            "Connector version override for definition %s with tag %s is not supported by the current version of Airbyte. "
                + "Required protocol version: %s. Supported range: %s - %s.",
            actorDefinitionId, overrideTag, protocolVersion.serialize(),
            protocolVersionRange.min().serialize(), protocolVersionRange.max().serialize()));
      }
      LOGGER.info("Using connector version override for definition {} with tag {}", actorDefinitionId, overrideTag);
    }

    return version;
  }

}
