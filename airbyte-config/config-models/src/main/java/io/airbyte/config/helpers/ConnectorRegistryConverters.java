/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers;

import io.airbyte.commons.version.AirbyteProtocolVersion;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.BreakingChangeScope;
import io.airbyte.config.ConnectorRegistryDestinationDefinition;
import io.airbyte.config.ConnectorRegistrySourceDefinition;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSourceDefinition.SourceType;
import io.airbyte.config.SupportLevel;
import io.airbyte.config.VersionBreakingChange;
import io.airbyte.protocol.models.ConnectorSpecification;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Utility class for converting between the connector registry and platform types.
 */
public class ConnectorRegistryConverters {

  /**
   * Convert the connector registry source type to the platform source definition type.
   */
  public static StandardSourceDefinition toStandardSourceDefinition(@Nullable final ConnectorRegistrySourceDefinition def) {
    if (def == null) {
      return null;
    }

    return new StandardSourceDefinition()
        .withSourceDefinitionId(def.getSourceDefinitionId())
        .withName(def.getName())
        .withIcon(def.getIcon())
        .withSourceType(toStandardSourceType(def.getSourceType()))
        .withTombstone(def.getTombstone())
        .withPublic(def.getPublic())
        .withCustom(def.getCustom())
        .withResourceRequirements(def.getResourceRequirements())
        .withMaxSecondsBetweenMessages(def.getMaxSecondsBetweenMessages());
  }

  /**
   * Convert the connector registry destination type to the platform destination definition type.
   */
  public static StandardDestinationDefinition toStandardDestinationDefinition(@Nullable final ConnectorRegistryDestinationDefinition def) {
    if (def == null) {
      return null;
    }

    return new StandardDestinationDefinition()
        .withDestinationDefinitionId(def.getDestinationDefinitionId())
        .withName(def.getName())
        .withIcon(def.getIcon())
        .withTombstone(def.getTombstone())
        .withPublic(def.getPublic())
        .withCustom(def.getCustom())
        .withResourceRequirements(def.getResourceRequirements());
  }

  /**
   * Convert the version-related fields of the ConnectorRegistrySourceDefinition into an
   * ActorDefinitionVersion.
   */
  public static ActorDefinitionVersion toActorDefinitionVersion(@Nullable final ConnectorRegistrySourceDefinition def) {
    if (def == null) {
      return null;
    }

    validateDockerImageTag(def.getDockerImageTag());
    return new ActorDefinitionVersion()
        .withActorDefinitionId(def.getSourceDefinitionId())
        .withDockerRepository(def.getDockerRepository())
        .withDockerImageTag(def.getDockerImageTag())
        .withSpec(def.getSpec())
        .withAllowedHosts(def.getAllowedHosts())
        .withDocumentationUrl(def.getDocumentationUrl())
        .withProtocolVersion(getProtocolVersion(def.getSpec()))
        .withReleaseDate(def.getReleaseDate())
        .withSupportLevel(def.getSupportLevel() == null ? SupportLevel.NONE : def.getSupportLevel())
        .withReleaseStage(def.getReleaseStage())
        .withSuggestedStreams(def.getSuggestedStreams());
  }

  /**
   * Convert the version-related fields of the ConnectorRegistrySourceDefinition into an
   * ActorDefinitionVersion.
   */
  public static ActorDefinitionVersion toActorDefinitionVersion(@Nullable final ConnectorRegistryDestinationDefinition def) {
    if (def == null) {
      return null;
    }

    validateDockerImageTag(def.getDockerImageTag());
    return new ActorDefinitionVersion()
        .withActorDefinitionId(def.getDestinationDefinitionId())
        .withDockerRepository(def.getDockerRepository())
        .withDockerImageTag(def.getDockerImageTag())
        .withSpec(def.getSpec())
        .withAllowedHosts(def.getAllowedHosts())
        .withDocumentationUrl(def.getDocumentationUrl())
        .withProtocolVersion(getProtocolVersion(def.getSpec()))
        .withReleaseDate(def.getReleaseDate())
        .withReleaseStage(def.getReleaseStage())
        .withSupportLevel(def.getSupportLevel() == null ? SupportLevel.NONE : def.getSupportLevel())
        .withNormalizationConfig(def.getNormalizationConfig())
        .withSupportsDbt(def.getSupportsDbt());
  }

  /**
   * Convert the breaking-change-related fields of the ConnectorRegistrySourceDefinition into a list
   * of ActorDefinitionBreakingChanges.
   */
  public static List<ActorDefinitionBreakingChange> toActorDefinitionBreakingChanges(@Nullable final ConnectorRegistrySourceDefinition def) {
    if (def == null || def.getReleases() == null || def.getReleases().getBreakingChanges() == null) {
      return Collections.emptyList();
    }

    final Map<String, VersionBreakingChange> breakingChangeMap = def.getReleases().getBreakingChanges().getAdditionalProperties();
    return toActorDefinitionBreakingChanges(breakingChangeMap, def.getSourceDefinitionId());
  }

  /**
   * Convert the breaking-change-related fields of the ConnectorRegistryDestinationDefinition into a
   * list of ActorDefinitionBreakingChanges.
   */
  public static List<ActorDefinitionBreakingChange> toActorDefinitionBreakingChanges(@Nullable final ConnectorRegistryDestinationDefinition def) {
    if (def == null || def.getReleases() == null || def.getReleases().getBreakingChanges() == null) {
      return Collections.emptyList();
    }

    final Map<String, VersionBreakingChange> breakingChangeMap = def.getReleases().getBreakingChanges().getAdditionalProperties();
    return toActorDefinitionBreakingChanges(breakingChangeMap, def.getDestinationDefinitionId());
  }

  private static List<ActorDefinitionBreakingChange> toActorDefinitionBreakingChanges(final Map<String, VersionBreakingChange> breakingChangeMap,
                                                                                      final UUID actorDefinitionID) {
    return breakingChangeMap.entrySet().stream()
        .map(entry -> new ActorDefinitionBreakingChange()
            .withActorDefinitionId(actorDefinitionID)
            .withVersion(new Version(entry.getKey()))
            .withMigrationDocumentationUrl(entry.getValue().getMigrationDocumentationUrl())
            .withUpgradeDeadline(entry.getValue().getUpgradeDeadline())
            .withMessage(entry.getValue().getMessage())
            .withScopedImpact(getValidatedScopedImpact(entry.getValue().getScopedImpact())))
        .collect(Collectors.toList());
  }

  private static SourceType toStandardSourceType(@Nullable final ConnectorRegistrySourceDefinition.SourceType sourceType) {
    if (sourceType == null) {
      return null;
    }

    switch (sourceType) {
      case API -> {
        return SourceType.API;
      }
      case FILE -> {
        return SourceType.FILE;
      }
      case DATABASE -> {
        return SourceType.DATABASE;
      }
      case CUSTOM -> {
        return SourceType.CUSTOM;
      }
      default -> throw new IllegalArgumentException("Unknown source type: " + sourceType);
    }
  }

  private static String getProtocolVersion(final ConnectorSpecification spec) {
    return AirbyteProtocolVersion.getWithDefault(spec != null ? spec.getProtocolVersion() : null).serialize();
  }

  private static void validateDockerImageTag(final String dockerImageTag) {
    if (dockerImageTag == null) {
      throw new IllegalArgumentException("dockerImageTag cannot be null");
    }
    try {
      new Version(dockerImageTag);
    } catch (final IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid Semver version for docker image tag: " + dockerImageTag, e);
    }
  }

  /**
   * jsonschema2Pojo does not support oneOf and const Therefore, the type checking for
   * BreakingChangeScope cannot take more specific subtypes. However, we want to validate that each
   * scope can be correctly resolved to an internal type that we'll use for processing later (e.g.
   * StreamBreakingChangeScope), So we validate that here at runtime instead.
   */
  private static List<BreakingChangeScope> getValidatedScopedImpact(final List<BreakingChangeScope> scopedImpact) {
    scopedImpact.forEach(BreakingChangeScopeFactory::validateBreakingChangeScope);
    return scopedImpact;
  }

}
