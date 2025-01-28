/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ReleaseStage;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.persistence.version_overrides.DefinitionVersionOverrideProvider;
import io.airbyte.data.services.ActorDefinitionService;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.annotation.Nullable;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for retrieving the actor definition version to use when running a connector. This
 * should be used when a specific actor or workspace is present, rather than accessing the fields
 * directly on the definitions.
 */
@Singleton
public class ActorDefinitionVersionHelper {

  /**
   * A wrapper class for returning the actor definition version and whether an override was applied.
   *
   * @param actorDefinitionVersion - actor definition version to use
   * @param isOverrideApplied - true if the version is the result of an override being applied,
   *        otherwise false
   */
  public record ActorDefinitionVersionWithOverrideStatus(ActorDefinitionVersion actorDefinitionVersion, boolean isOverrideApplied) {}

  private static final Logger LOGGER = LoggerFactory.getLogger(ActorDefinitionVersionHelper.class);

  private final ActorDefinitionService actorDefinitionService;
  private final DefinitionVersionOverrideProvider configOverrideProvider;

  public ActorDefinitionVersionHelper(final ActorDefinitionService actorDefinitionService,
                                      @Named("configurationVersionOverrideProvider") final DefinitionVersionOverrideProvider configOverrideProvider) {
    this.actorDefinitionService = actorDefinitionService;
    this.configOverrideProvider = configOverrideProvider;

    LOGGER.info("ActorDefinitionVersionHelper initialized with override provider: {}",
        configOverrideProvider.getClass().getSimpleName());
  }

  private ActorDefinitionVersion getDefaultSourceVersion(final StandardSourceDefinition sourceDefinition)
      throws IOException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final UUID versionId = sourceDefinition.getDefaultVersionId();

    if (versionId == null) {
      throw new RuntimeException(String.format("Default version for source is not set (Definition ID: %s)",
          sourceDefinition.getSourceDefinitionId()));
    }

    return actorDefinitionService.getActorDefinitionVersion(versionId);
  }

  private ActorDefinitionVersion getDefaultDestinationVersion(final StandardDestinationDefinition destinationDefinition)
      throws IOException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final UUID versionId = destinationDefinition.getDefaultVersionId();

    if (versionId == null) {
      throw new RuntimeException(String.format("Default version for destination is not set (Definition ID: %s)",
          destinationDefinition.getDestinationDefinitionId()));
    }

    return actorDefinitionService.getActorDefinitionVersion(versionId);
  }

  /**
   * Getting versions from a list of definitions.
   *
   * @param shownSourceDefs Definitions to get versions for.
   * @param workspaceId UUID of the workspace, not currently used
   *
   * @return Map of ids to definition versions
   */
  public Map<UUID, ActorDefinitionVersion> getSourceVersions(List<StandardSourceDefinition> shownSourceDefs, UUID workspaceId) {
    try {
      var overrides = configOverrideProvider.getOverrides(
          shownSourceDefs
              .stream()
              .map(StandardSourceDefinition::getSourceDefinitionId)
              .toList(),
          workspaceId)
          // Map to DefinitionId, DefinitionVersion
          .stream()
          .collect(
              Collectors.toMap(
                  defWithOverride -> defWithOverride.actorDefinitionVersion.getActorDefinitionId(),
                  defWithOverride -> defWithOverride.actorDefinitionVersion));

      // Get all the actorDefinitionVersions for definitions that do not have an override.
      var sourceVersions = actorDefinitionService.getActorDefinitionVersions(
          shownSourceDefs
              .stream()
              // Filter out definitions that have a version override
              .filter(version -> overrides.get(version.getSourceDefinitionId()) == null)
              .map(StandardSourceDefinition::getDefaultVersionId)
              .toList())
          .stream()
          .collect(Collectors.toMap(ActorDefinitionVersion::getActorDefinitionId, Function.identity()));

      // Merge overrides and non-overrides together
      sourceVersions.putAll(overrides);

      return sourceVersions;
    } catch (IOException e) {
      LOGGER.error(e.getLocalizedMessage());
      throw new RuntimeException(e);
    }
  }

  /**
   * Getting versions from a list of definitions.
   *
   * @param shownDestinationDefs Definitions to get versions for.
   * @param workspaceId UUID of the workspace, not currently used
   *
   * @return Map of ids to definition versions
   */
  public Map<UUID, ActorDefinitionVersion> getDestinationVersions(List<StandardDestinationDefinition> shownDestinationDefs, UUID workspaceId) {
    try {
      var overrides = configOverrideProvider.getOverrides(
          shownDestinationDefs
              .stream()
              .map(StandardDestinationDefinition::getDestinationDefinitionId)
              .toList(),
          workspaceId)
          // Map to DefinitionId, DefinitionVersion
          .stream()
          .collect(
              Collectors.toMap(
                  defWithOverride -> defWithOverride.actorDefinitionVersion.getActorDefinitionId(),
                  defWithOverride -> defWithOverride.actorDefinitionVersion));

      // Get all the actorDefinitionVersions for definitions that do not have an override.
      var destinationVersions = actorDefinitionService.getActorDefinitionVersions(
          shownDestinationDefs
              .stream()
              // Filter out definitions that have a version override
              .filter(version -> overrides.get(version.getDestinationDefinitionId()) == null)
              .map(StandardDestinationDefinition::getDefaultVersionId)
              .toList())
          .stream()
          .collect(Collectors.toMap(ActorDefinitionVersion::getActorDefinitionId, Function.identity()));

      // Merge overrides and non-overrides together
      destinationVersions.putAll(overrides);

      return destinationVersions;
    } catch (IOException e) {
      LOGGER.error(e.getLocalizedMessage());
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the actor definition version to use for a source, and whether an override was applied.
   *
   * @param sourceDefinition source definition
   * @param workspaceId workspace id
   * @param actorId source id
   * @return actor definition version with override status
   */
  public ActorDefinitionVersionWithOverrideStatus getSourceVersionWithOverrideStatus(final StandardSourceDefinition sourceDefinition,
                                                                                     final UUID workspaceId,
                                                                                     @Nullable final UUID actorId)
      throws IOException, JsonValidationException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final ActorDefinitionVersion defaultVersion = getDefaultSourceVersion(sourceDefinition);

    final Optional<ActorDefinitionVersionWithOverrideStatus> versionOverride = configOverrideProvider.getOverride(
        sourceDefinition.getSourceDefinitionId(),
        workspaceId,
        actorId);

    return versionOverride.orElse(new ActorDefinitionVersionWithOverrideStatus(defaultVersion, false));
  }

  /**
   * Get the actor definition version to use for a source.
   *
   * @param sourceDefinition source definition
   * @param workspaceId workspace id
   * @param actorId source id
   * @return actor definition version
   */
  public ActorDefinitionVersion getSourceVersion(final StandardSourceDefinition sourceDefinition,
                                                 final UUID workspaceId,
                                                 @Nullable final UUID actorId)
      throws IOException, JsonValidationException, io.airbyte.data.exceptions.ConfigNotFoundException {
    return getSourceVersionWithOverrideStatus(sourceDefinition, workspaceId, actorId).actorDefinitionVersion();
  }

  /**
   * Get the actor definition version to use for sources in a given workspace.
   *
   * @param sourceDefinition source definition
   * @param workspaceId workspace id
   * @return actor definition version
   */
  public ActorDefinitionVersion getSourceVersion(final StandardSourceDefinition sourceDefinition, final UUID workspaceId)
      throws ConfigNotFoundException, IOException, JsonValidationException, io.airbyte.data.exceptions.ConfigNotFoundException {
    return getSourceVersion(sourceDefinition, workspaceId, null);
  }

  /**
   * Get the actor definition version to use for a destination, and whether an override was applied.
   *
   * @param destinationDefinition destination definition
   * @param workspaceId workspace id
   * @param actorId destination id
   * @return actor definition version with override status
   */
  public ActorDefinitionVersionWithOverrideStatus getDestinationVersionWithOverrideStatus(final StandardDestinationDefinition destinationDefinition,
                                                                                          final UUID workspaceId,
                                                                                          @Nullable final UUID actorId)
      throws IOException, JsonValidationException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final ActorDefinitionVersion defaultVersion = getDefaultDestinationVersion(destinationDefinition);

    final Optional<ActorDefinitionVersionWithOverrideStatus> versionOverride = configOverrideProvider.getOverride(
        destinationDefinition.getDestinationDefinitionId(),
        workspaceId,
        actorId);

    return versionOverride.orElse(new ActorDefinitionVersionWithOverrideStatus(defaultVersion, false));
  }

  /**
   * Get the actor definition version to use for a destination.
   *
   * @param destinationDefinition destination definition
   * @param workspaceId workspace id
   * @param actorId destination id
   * @return actor definition version
   */
  public ActorDefinitionVersion getDestinationVersion(final StandardDestinationDefinition destinationDefinition,
                                                      final UUID workspaceId,
                                                      @Nullable final UUID actorId)
      throws IOException, JsonValidationException, io.airbyte.data.exceptions.ConfigNotFoundException {
    return getDestinationVersionWithOverrideStatus(destinationDefinition, workspaceId, actorId).actorDefinitionVersion();
  }

  /**
   * Get the actor definition version to use for destinations in a given workspace.
   *
   * @param destinationDefinition destination definition
   * @param workspaceId workspace id
   * @return actor definition version
   */
  public ActorDefinitionVersion getDestinationVersion(final StandardDestinationDefinition destinationDefinition,
                                                      final UUID workspaceId)
      throws IOException, JsonValidationException, io.airbyte.data.exceptions.ConfigNotFoundException {
    return getDestinationVersion(destinationDefinition, workspaceId, null);
  }

  /**
   * Get the docker image name (docker_repository:docker_image_tag) for a given actor definition
   * version.
   *
   * @param actorDefinitionVersion actor definition version
   * @return docker image name
   */
  public static String getDockerImageName(final ActorDefinitionVersion actorDefinitionVersion) {
    return String.format("%s:%s", actorDefinitionVersion.getDockerRepository(), actorDefinitionVersion.getDockerImageTag());
  }

  /**
   * Helper method to share eligibility logic for free connector program.
   *
   * @param actorDefinitionVersions List of versions that should be checked for alpha/beta status
   * @return true if any of the provided versions is in alpha or beta
   */
  public static boolean hasAlphaOrBetaVersion(final List<ActorDefinitionVersion> actorDefinitionVersions) {
    return actorDefinitionVersions.stream()
        .anyMatch(version -> version.getReleaseStage().equals(ReleaseStage.ALPHA) || version.getReleaseStage().equals(ReleaseStage.BETA));
  }

}
