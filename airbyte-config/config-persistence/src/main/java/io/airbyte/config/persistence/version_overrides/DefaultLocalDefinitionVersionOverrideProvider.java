/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence.version_overrides;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.commons.util.MoreIterators;
import io.airbyte.commons.yaml.Yamls;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorDefinitionVersionOverride;
import io.airbyte.config.ActorType;
import io.airbyte.config.VersionOverride;
import io.airbyte.config.specs.GcsBucketSpecFetcher;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.micronaut.core.annotation.Creator;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.util.StringUtils;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link DefinitionVersionOverrideProvider} that reads the overrides from
 * a YAML file in the classpath.
 */
@Singleton
public class DefaultLocalDefinitionVersionOverrideProvider implements LocalDefinitionVersionOverrideProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultLocalDefinitionVersionOverrideProvider.class);

  private final GcsBucketSpecFetcher gcsBucketSpecFetcher;

  private final Map<UUID, ActorDefinitionVersionOverride> overrideMap;

  @Creator
  public DefaultLocalDefinitionVersionOverrideProvider(final GcsBucketSpecFetcher gcsBucketSpecFetcher) {
    this(DefaultLocalDefinitionVersionOverrideProvider.class, "version_overrides.yml", gcsBucketSpecFetcher);
    LOGGER.info("Initialized default definition version overrides");
  }

  public DefaultLocalDefinitionVersionOverrideProvider(final Class<?> resourceClass,
                                                       final String resourceName,
                                                       final GcsBucketSpecFetcher gcsBucketSpecFetcher) {
    this.overrideMap = getLocalOverrides(resourceClass, resourceName);
    this.gcsBucketSpecFetcher = gcsBucketSpecFetcher;
  }

  @VisibleForTesting
  public DefaultLocalDefinitionVersionOverrideProvider(final Map<UUID, ActorDefinitionVersionOverride> overrideMap,
                                                       final GcsBucketSpecFetcher gcsBucketSpecFetcher) {
    this.overrideMap = overrideMap;
    this.gcsBucketSpecFetcher = gcsBucketSpecFetcher;
  }

  private Map<UUID, ActorDefinitionVersionOverride> getLocalOverrides(final Class<?> resourceClass, final String resourceName) {
    try {
      final String overridesStr = MoreResources.readResource(resourceClass, resourceName);
      final JsonNode overridesList = Yamls.deserialize(overridesStr);
      return MoreIterators.toList(overridesList.elements()).stream().collect(Collectors.toMap(
          json -> UUID.fromString(json.get("actorDefinitionId").asText()),
          json -> Jsons.object(json, ActorDefinitionVersionOverride.class)));
    } catch (final Exception e) {
      LOGGER.error("Failed to read local actor definition version overrides file", e);
      return Map.of();
    }
  }

  @Override
  public Optional<ActorDefinitionVersion> getOverride(final ActorType actorType,
                                                      final UUID actorDefinitionId,
                                                      final UUID workspaceId,
                                                      @Nullable final UUID actorId,
                                                      final ActorDefinitionVersion defaultVersion) {
    Optional<ActorDefinitionVersion> localOverride = Optional.empty();

    if (actorId != null) {
      localOverride = getOverrideForTarget(actorDefinitionId, actorId, OverrideTargetType.ACTOR, defaultVersion);
    }

    if (localOverride.isEmpty()) {
      localOverride = getOverrideForTarget(actorDefinitionId, workspaceId, OverrideTargetType.WORKSPACE, defaultVersion);
    }

    return localOverride;
  }

  /**
   * Returns the overridden ActorDefinitionVersion for a given target, if one exists. Otherwise, an
   * empty optional is returned.
   */
  public Optional<ActorDefinitionVersion> getOverrideForTarget(final UUID actorDefinitionId,
                                                               final UUID targetId,
                                                               final OverrideTargetType targetType,
                                                               final ActorDefinitionVersion defaultVersion) {
    if (overrideMap.containsKey(actorDefinitionId)) {
      final ActorDefinitionVersionOverride override = overrideMap.get(actorDefinitionId);
      for (final VersionOverride versionOverride : override.getVersionOverrides()) {
        final List<UUID> targetIds = switch (targetType) {
          case ACTOR -> versionOverride.getActorIds();
          case WORKSPACE -> versionOverride.getWorkspaceIds();
        };

        if (targetIds != null && targetIds.contains(targetId)) {
          final ActorDefinitionVersion version = versionOverride.getActorDefinitionVersion();

          if (StringUtils.isEmpty(version.getDockerImageTag())) {
            LOGGER.warn("Invalid version override for {} {} with {} id {}. Falling back to default version.", override.getActorType(),
                actorDefinitionId, targetType.getName(), targetId);
            return Optional.empty();
          }

          if (StringUtils.isEmpty(version.getDockerRepository())) {
            version.setDockerRepository(defaultVersion.getDockerRepository());
          }

          if (version.getSpec() == null) {
            final Optional<ConnectorSpecification> spec = gcsBucketSpecFetcher.attemptFetch(
                String.format("%s:%s", version.getDockerRepository(), version.getDockerImageTag()));

            if (spec.isPresent()) {
              version.setSpec(spec.get());
              LOGGER.info("Fetched spec from remote cache for {} {} version override ({}).",
                  override.getActorType(), actorDefinitionId, version.getDockerImageTag());
            } else {
              LOGGER.error("Failed to fetch spec from remote cache for {} {} version override ({}). Falling back to default version.",
                  override.getActorType(), actorDefinitionId, version.getDockerImageTag());
              return Optional.empty();
            }
          }

          LOGGER.info("Using version override for {} {} with {} id {}: {}", override.getActorType(), actorDefinitionId, targetType.getName(),
              targetId, version.getDockerImageTag());
          return Optional.of(version);
        }
      }
    }

    return Optional.empty();
  }

}
