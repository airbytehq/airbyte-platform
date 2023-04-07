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
import io.airbyte.config.VersionOverride;
import io.micronaut.core.annotation.Creator;
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
 * Default implementation of {@link LocalDefinitionVersionOverrideProvider} that reads the overrides
 * from a YAML file in the classpath.
 */
@Singleton
public class DefaultDefinitionVersionOverrideProvider implements LocalDefinitionVersionOverrideProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultDefinitionVersionOverrideProvider.class);

  private final Map<UUID, ActorDefinitionVersionOverride> overrideMap;

  @Creator
  public DefaultDefinitionVersionOverrideProvider() {
    this(DefaultDefinitionVersionOverrideProvider.class, "version_overrides.yml");
    LOGGER.info("Initialized default definition version overrides");
  }

  public DefaultDefinitionVersionOverrideProvider(final Class<?> resourceClass, final String resourceName) {
    this.overrideMap = getLocalOverrides(resourceClass, resourceName);
  }

  @VisibleForTesting
  public DefaultDefinitionVersionOverrideProvider(final Map<UUID, ActorDefinitionVersionOverride> overrideMap) {
    this.overrideMap = overrideMap;
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
  public Optional<ActorDefinitionVersion> getOverride(final UUID actorDefinitionId,
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

          if (StringUtils.isEmpty(version.getDockerImageTag()) || version.getSpec() == null) {
            LOGGER.warn("Invalid version override for {} {} with {} id {}. Falling back to default version.", override.getActorType(),
                actorDefinitionId, targetType.getName(), targetId);
            return Optional.empty();
          }

          if (StringUtils.isEmpty(version.getDockerRepository())) {
            version.setDockerRepository(defaultVersion.getDockerRepository());
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
