/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence.version_overrides;

import io.airbyte.commons.version.AirbyteProtocolVersionRange;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.persistence.ActorDefinitionVersionResolver;
import io.airbyte.featureflag.ConnectorVersionOverride;
import io.airbyte.featureflag.Context;
import io.airbyte.featureflag.Destination;
import io.airbyte.featureflag.DestinationDefinition;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.Multi;
import io.airbyte.featureflag.Source;
import io.airbyte.featureflag.SourceDefinition;
import io.airbyte.featureflag.Workspace;
import io.micronaut.context.annotation.Requires;
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
@Requires(bean = ActorDefinitionVersionResolver.class)
public class DefaultDefinitionVersionOverrideProvider implements DefinitionVersionOverrideProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultDefinitionVersionOverrideProvider.class);
  private final ActorDefinitionVersionResolver actorDefinitionVersionResolver;
  private final FeatureFlagClient featureFlagClient;
  private final AirbyteProtocolVersionRange protocolVersionRange;

  public DefaultDefinitionVersionOverrideProvider(final ActorDefinitionVersionResolver actorDefinitionVersionResolver,
                                                  final FeatureFlagClient featureFlagClient,
                                                  final AirbyteProtocolVersionRange protocolVersionRange) {
    this.actorDefinitionVersionResolver = actorDefinitionVersionResolver;
    this.featureFlagClient = featureFlagClient;
    this.protocolVersionRange = protocolVersionRange;
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

  @Override
  public Optional<ActorDefinitionVersion> getOverride(final ActorType actorType,
                                                      final UUID actorDefinitionId,
                                                      final UUID workspaceId,
                                                      @Nullable final UUID actorId,
                                                      final ActorDefinitionVersion defaultVersion) {
    final List<Context> contexts = getContexts(actorType, actorDefinitionId, workspaceId, actorId);
    final String overrideTag = featureFlagClient.stringVariation(ConnectorVersionOverride.INSTANCE, new Multi(contexts));

    if (StringUtils.isEmpty(overrideTag)) {
      return Optional.empty();
    }

    try {
      final Optional<ActorDefinitionVersion> version =
          actorDefinitionVersionResolver.resolveVersionForTag(actorDefinitionId, actorType, defaultVersion.getDockerRepository(), overrideTag);
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

    } catch (final IOException e) {
      LOGGER.error("Failed to read or persist actor definition version for definition {} with tag {}", actorDefinitionId,
          defaultVersion.getDockerRepository(), e);
      return Optional.empty();
    }
  }

}
