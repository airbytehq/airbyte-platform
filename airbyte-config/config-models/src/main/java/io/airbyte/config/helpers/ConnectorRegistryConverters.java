/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers;

import io.airbyte.commons.version.AirbyteProtocolVersion;
import io.airbyte.commons.version.Version;
import io.airbyte.config.AbInternal;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.BreakingChangeScope;
import io.airbyte.config.ConnectorEnumRolloutState;
import io.airbyte.config.ConnectorPackageInfo;
import io.airbyte.config.ConnectorRegistryDestinationDefinition;
import io.airbyte.config.ConnectorRegistryEntryGeneratedFields;
import io.airbyte.config.ConnectorRegistryEntryMetrics;
import io.airbyte.config.ConnectorRegistrySourceDefinition;
import io.airbyte.config.ConnectorRollout;
import io.airbyte.config.RolloutConfiguration;
import io.airbyte.config.SourceFileInfo;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSourceDefinition.SourceType;
import io.airbyte.config.SupportLevel;
import io.airbyte.config.VersionBreakingChange;
import io.airbyte.protocol.models.v0.ConnectorSpecification;
import jakarta.annotation.Nullable;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Utility class for converting between the connector registry and platform types.
 */
public class ConnectorRegistryConverters {

  public static final RolloutConfiguration DEFAULT_ROLLOUT_CONFIGURATION =
      new RolloutConfiguration().withInitialPercentage(0L).withMaxPercentage(0L).withAdvanceDelayMinutes(0L);

  /**
   * Convert the connector registry source type to the platform source definition type.
   */
  public static StandardSourceDefinition toStandardSourceDefinition(@Nullable final ConnectorRegistrySourceDefinition def) {
    if (def == null) {
      return null;
    }

    final ConnectorRegistryEntryMetrics metrics = Optional.of(def)
        .map(ConnectorRegistrySourceDefinition::getGenerated)
        .map(ConnectorRegistryEntryGeneratedFields::getMetrics)
        .orElse(null);

    return new StandardSourceDefinition()
        .withSourceDefinitionId(def.getSourceDefinitionId())
        .withName(def.getName())
        .withIcon(def.getIcon())
        .withIconUrl(def.getIconUrl())
        .withSourceType(toStandardSourceType(def.getSourceType()))
        .withTombstone(def.getTombstone())
        .withPublic(def.getPublic())
        .withCustom(def.getCustom())
        .withEnterprise(def.getAbInternal() != null ? def.getAbInternal().getIsEnterprise() : false)
        .withMetrics(metrics)
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

    final ConnectorRegistryEntryMetrics metrics = Optional.of(def)
        .map(ConnectorRegistryDestinationDefinition::getGenerated)
        .map(ConnectorRegistryEntryGeneratedFields::getMetrics)
        .orElse(null);

    return new StandardDestinationDefinition()
        .withDestinationDefinitionId(def.getDestinationDefinitionId())
        .withName(def.getName())
        .withIcon(def.getIcon())
        .withIconUrl(def.getIconUrl())
        .withTombstone(def.getTombstone())
        .withPublic(def.getPublic())
        .withCustom(def.getCustom())
        .withEnterprise(def.getAbInternal() != null ? def.getAbInternal().getIsEnterprise() : false)
        .withMetrics(metrics)
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

    final Date lastModified = Optional.of(def)
        .map(ConnectorRegistrySourceDefinition::getGenerated)
        .map(ConnectorRegistryEntryGeneratedFields::getSourceFileInfo)
        .map(SourceFileInfo::getMetadataLastModified)
        .orElse(null);

    final String cdkVersion = Optional.of(def)
        .map(ConnectorRegistrySourceDefinition::getPackageInfo)
        .map(ConnectorPackageInfo::getCdkVersion)
        .orElse(null);

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
        .withInternalSupportLevel(Optional.ofNullable(def.getAbInternal()).map(AbInternal::getSl).orElse(100L))
        .withReleaseStage(def.getReleaseStage())
        .withLastPublished(lastModified)
        .withCdkVersion(cdkVersion)
        .withSuggestedStreams(def.getSuggestedStreams())
        .withLanguage(def.getLanguage())
        .withSupportsFileTransfer(def.getSupportsFileTransfer())
        .withConnectorIPCOptions(def.getConnectorIPCOptions());

  }

  /**
   * Convert the version-related fields of the ConnectorRegistrySourceDefinition into an
   * ActorDefinitionVersion.
   */
  public static ActorDefinitionVersion toActorDefinitionVersion(@Nullable final ConnectorRegistryDestinationDefinition def) {
    if (def == null) {
      return null;
    }

    final Date lastModified = Optional.of(def)
        .map(ConnectorRegistryDestinationDefinition::getGenerated)
        .map(ConnectorRegistryEntryGeneratedFields::getSourceFileInfo)
        .map(SourceFileInfo::getMetadataLastModified)
        .orElse(null);

    final String cdkVersion = Optional.of(def)
        .map(ConnectorRegistryDestinationDefinition::getPackageInfo)
        .map(ConnectorPackageInfo::getCdkVersion)
        .orElse(null);

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
        .withInternalSupportLevel(Optional.ofNullable(def.getAbInternal()).map(AbInternal::getSl).orElse(100L))
        .withLastPublished(lastModified)
        .withCdkVersion(cdkVersion)
        .withSupportsRefreshes(def.getSupportsRefreshes() != null && def.getSupportsRefreshes())
        .withLanguage(def.getLanguage())
        .withSupportsFileTransfer(def.getSupportsFileTransfer())
        .withSupportsDataActivation(def.getSupportsDataActivation())
        .withConnectorIPCOptions(def.getConnectorIPCOptions());

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
            .withDeadlineAction(entry.getValue().getDeadlineAction())
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

  public static ConnectorRollout toConnectorRollout(final ConnectorRegistrySourceDefinition rcDef,
                                                    final ActorDefinitionVersion rcAdv,
                                                    final ActorDefinitionVersion initialAdv) {
    assert rcDef.getSourceDefinitionId().equals(rcAdv.getActorDefinitionId());
    assert Objects.equals(rcDef.getDockerRepository(), rcAdv.getDockerRepository());
    assert Objects.equals(rcDef.getDockerImageTag(), rcAdv.getDockerImageTag());
    if (rcDef.getReleases() != null) {
      boolean hasBreakingChange = rcDef.getReleases().getBreakingChanges() != null
          && rcDef.getReleases().getBreakingChanges().getAdditionalProperties().containsKey(rcDef.getDockerImageTag());
      RolloutConfiguration rolloutConfiguration = rcDef.getReleases().getRolloutConfiguration();
      return ConnectorRegistryConverters.toConnectorRollout(rolloutConfiguration, rcAdv, initialAdv, hasBreakingChange);
    }
    return ConnectorRegistryConverters.toConnectorRollout(null, rcAdv, initialAdv, false);
  }

  public static ConnectorRollout toConnectorRollout(final ConnectorRegistryDestinationDefinition rcDef,
                                                    final ActorDefinitionVersion rcAdv,
                                                    final ActorDefinitionVersion initialAdv) {
    assert rcDef.getDestinationDefinitionId().equals(rcAdv.getActorDefinitionId());
    assert Objects.equals(rcDef.getDockerRepository(), rcAdv.getDockerRepository());
    assert Objects.equals(rcDef.getDockerImageTag(), rcAdv.getDockerImageTag());
    if (rcDef.getReleases() != null) {
      boolean hasBreakingChange = rcDef.getReleases().getBreakingChanges() != null
          && rcDef.getReleases().getBreakingChanges().getAdditionalProperties().containsKey(rcDef.getDockerImageTag());
      RolloutConfiguration rolloutConfiguration = rcDef.getReleases().getRolloutConfiguration();
      return ConnectorRegistryConverters.toConnectorRollout(rolloutConfiguration, rcAdv, initialAdv, hasBreakingChange);
    }
    return ConnectorRegistryConverters.toConnectorRollout(null, rcAdv, initialAdv, false);
  }

  private static ConnectorRollout toConnectorRollout(final RolloutConfiguration rolloutConfiguration,
                                                     final ActorDefinitionVersion rcAdv,
                                                     final ActorDefinitionVersion initialAdv,
                                                     final boolean hasBreakingChange) {
    ConnectorRollout connectorRollout = new ConnectorRollout(
        UUID.randomUUID(),
        null,
        rcAdv.getActorDefinitionId(),
        rcAdv.getVersionId(),
        initialAdv.getVersionId(),
        ConnectorEnumRolloutState.INITIALIZED,
        (rolloutConfiguration != null ? rolloutConfiguration.getInitialPercentage() : DEFAULT_ROLLOUT_CONFIGURATION.getInitialPercentage())
            .intValue(),
        null,
        (rolloutConfiguration != null ? rolloutConfiguration.getMaxPercentage() : DEFAULT_ROLLOUT_CONFIGURATION.getMaxPercentage()).intValue(),
        hasBreakingChange,
        null,
        (rolloutConfiguration != null ? rolloutConfiguration.getAdvanceDelayMinutes() : DEFAULT_ROLLOUT_CONFIGURATION.getAdvanceDelayMinutes())
            .intValue(),
        null,
        OffsetDateTime.now().toEpochSecond(),
        OffsetDateTime.now().toEpochSecond(),
        null,
        null,
        null,
        null,
        null,
        null,
        null);

    return connectorRollout;
  }

  public static List<ConnectorRegistrySourceDefinition> toRcSourceDefinitions(@Nullable final ConnectorRegistrySourceDefinition def) {
    if (def == null || def.getReleases() == null || def.getReleases().getReleaseCandidates() == null) {
      return Collections.emptyList();
    }

    return def
        .getReleases()
        .getReleaseCandidates()
        .getAdditionalProperties().values()
        .stream()
        .filter(Objects::nonNull)
        .toList();
  }

  public static List<ConnectorRegistryDestinationDefinition> toRcDestinationDefinitions(@Nullable final ConnectorRegistryDestinationDefinition def) {
    if (def == null || def.getReleases() == null || def.getReleases().getReleaseCandidates() == null) {
      return Collections.emptyList();
    }

    return def
        .getReleases()
        .getReleaseCandidates()
        .getAdditionalProperties().values()
        .stream()
        .filter(Objects::nonNull)
        .toList();
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
