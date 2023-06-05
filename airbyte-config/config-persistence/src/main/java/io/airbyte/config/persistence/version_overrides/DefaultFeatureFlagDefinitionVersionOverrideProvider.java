/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence.version_overrides;

import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.config.specs.GcsBucketSpecFetcher;
import io.airbyte.featureflag.ConnectorVersionOverride;
import io.airbyte.featureflag.Context;
import io.airbyte.featureflag.Destination;
import io.airbyte.featureflag.DestinationDefinition;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.Multi;
import io.airbyte.featureflag.Source;
import io.airbyte.featureflag.SourceDefinition;
import io.airbyte.featureflag.Workspace;
import io.airbyte.protocol.models.ConnectorSpecification;
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
public class DefaultFeatureFlagDefinitionVersionOverrideProvider implements FeatureFlagDefinitionVersionOverrideProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultFeatureFlagDefinitionVersionOverrideProvider.class);

  private final ConfigRepository configRepository;
  private final GcsBucketSpecFetcher gcsBucketSpecFetcher;
  private final FeatureFlagClient featureFlagClient;

  public DefaultFeatureFlagDefinitionVersionOverrideProvider(final ConfigRepository configRepository,
                                                             final GcsBucketSpecFetcher gcsBucketSpecFetcher,
                                                             final FeatureFlagClient featureFlagClient) {
    this.configRepository = configRepository;
    this.gcsBucketSpecFetcher = gcsBucketSpecFetcher;
    this.featureFlagClient = featureFlagClient;
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

  /**
   * Resolves an ActorDefinitionVersion from the database for a given docker image tag. If the version
   * is not found in the database, it will attempt to fetch the spec from the remote cache and create
   * a new ADV.
   *
   * @return ActorDefinitionVersion if the version was resolved, otherwise empty optional
   */
  private Optional<ActorDefinitionVersion> resolveVersionForTag(final UUID actorDefinitionId,
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

      final ActorDefinitionVersion newVersion = new ActorDefinitionVersion()
          .withActorDefinitionId(actorDefinitionId)
          .withDockerImageTag(dockerImageTag)
          .withDockerRepository(defaultVersion.getDockerRepository())
          .withAllowedHosts(defaultVersion.getAllowedHosts())
          .withDocumentationUrl(defaultVersion.getDocumentationUrl())
          .withNormalizationConfig(defaultVersion.getNormalizationConfig())
          .withReleaseDate(defaultVersion.getReleaseDate())
          .withReleaseStage(defaultVersion.getReleaseStage())
          .withSuggestedStreams(defaultVersion.getSuggestedStreams())
          .withSupportsDbt(defaultVersion.getSupportsDbt())
          .withProtocolVersion(defaultVersion.getProtocolVersion());

      final Optional<ConnectorSpecification> spec = gcsBucketSpecFetcher.attemptFetch(
          String.format("%s:%s", newVersion.getDockerRepository(), newVersion.getDockerImageTag()));

      if (spec.isPresent()) {
        LOGGER.info("Fetched spec from remote cache for {}:{}.", newVersion.getDockerRepository(), newVersion.getDockerImageTag());
        newVersion.setSpec(spec.get());
      } else {
        LOGGER.error("Failed to fetch spec from remote cache for version override {}:{}", newVersion.getDockerRepository(),
            newVersion.getDockerImageTag());
        return Optional.empty();
      }

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
    final Optional<ActorDefinitionVersion> version = resolveVersionForTag(actorDefinitionId, overrideTag, defaultVersion);
    if (version.isPresent()) {
      LOGGER.info("Using connector version override for definition {} with tag {}", actorDefinitionId, overrideTag);
    }

    return version;
  }

}
