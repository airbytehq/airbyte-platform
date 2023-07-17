/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers;

import io.airbyte.commons.version.AirbyteProtocolVersion;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ConnectorRegistryDestinationDefinition;
import io.airbyte.config.ConnectorRegistrySourceDefinition;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSourceDefinition.SourceType;
import io.airbyte.protocol.models.ConnectorSpecification;
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

    return new ActorDefinitionVersion()
        .withActorDefinitionId(def.getSourceDefinitionId())
        .withDockerRepository(def.getDockerRepository())
        .withDockerImageTag(def.getDockerImageTag())
        .withSpec(def.getSpec())
        .withAllowedHosts(def.getAllowedHosts())
        .withDocumentationUrl(def.getDocumentationUrl())
        .withProtocolVersion(getProtocolVersion(def.getSpec()))
        .withReleaseDate(def.getReleaseDate())
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
        .withNormalizationConfig(def.getNormalizationConfig())
        .withSupportsDbt(def.getSupportsDbt());
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

}
