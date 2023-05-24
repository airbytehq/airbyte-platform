/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers;

import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ConnectorRegistryDestinationDefinition;
import io.airbyte.config.ConnectorRegistrySourceDefinition;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSourceDefinition.SourceType;
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
        .withDockerRepository(def.getDockerRepository())
        .withDockerImageTag(def.getDockerImageTag())
        .withDocumentationUrl(def.getDocumentationUrl())
        .withIcon(def.getIcon())
        .withSourceType(toStandardSourceType(def.getSourceType()))
        .withSpec(def.getSpec())
        .withTombstone(def.getTombstone())
        .withPublic(def.getPublic())
        .withCustom(def.getCustom())
        .withReleaseDate(def.getReleaseDate())
        .withReleaseStage(def.getReleaseStage())
        .withResourceRequirements(def.getResourceRequirements())
        .withProtocolVersion(def.getProtocolVersion())
        .withAllowedHosts(def.getAllowedHosts())
        .withSuggestedStreams(def.getSuggestedStreams())
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
        .withDockerRepository(def.getDockerRepository())
        .withDockerImageTag(def.getDockerImageTag())
        .withDocumentationUrl(def.getDocumentationUrl())
        .withIcon(def.getIcon())
        .withSpec(def.getSpec())
        .withTombstone(def.getTombstone())
        .withPublic(def.getPublic())
        .withCustom(def.getCustom())
        .withReleaseDate(def.getReleaseDate())
        .withReleaseStage(def.getReleaseStage())
        .withResourceRequirements(def.getResourceRequirements())
        .withProtocolVersion(def.getProtocolVersion())
        .withAllowedHosts(def.getAllowedHosts())
        .withNormalizationConfig(def.getNormalizationConfig())
        .withSupportsDbt(def.getSupportsDbt());
  }

  /**
   * Convert the version-related fields of the ConnectorRegistrySourceDefinition into an
   * ActorDefinitionVersion.
   */
  public static ActorDefinitionVersion toActorDefinitionVersion(@Nullable final ConnectorRegistrySourceDefinition def) {
    if (def == null) {
      return null;
    }

    return new ActorDefinitionVersion()
        .withActorDefinitionId(def.getSourceDefinitionId())
        .withDockerRepository(def.getDockerRepository())
        .withDockerImageTag(def.getDockerImageTag())
        .withSpec(def.getSpec());
  }

  /**
   * Convert the version-related fields of the ConnectorRegistrySourceDefinition into an
   * ActorDefinitionVersion.
   */
  public static ActorDefinitionVersion toActorDefinitionVersion(@Nullable final ConnectorRegistryDestinationDefinition def) {
    if (def == null) {
      return null;
    }

    return new ActorDefinitionVersion()
        .withActorDefinitionId(def.getDestinationDefinitionId())
        .withDockerRepository(def.getDockerRepository())
        .withDockerImageTag(def.getDockerImageTag())
        .withSpec(def.getSpec());
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

}
