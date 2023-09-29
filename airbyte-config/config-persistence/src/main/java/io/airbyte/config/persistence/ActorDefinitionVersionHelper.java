/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.ReleaseStage;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.persistence.ConfigRepository.StandardSyncQuery;
import io.airbyte.config.persistence.version_overrides.DefinitionVersionOverrideProvider;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.UseActorScopedDefaultVersions;
import io.airbyte.featureflag.Workspace;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;
import kotlin.Pair;
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

  private final ConfigRepository configRepository;
  private final DefinitionVersionOverrideProvider overrideProvider;
  private final FeatureFlagClient featureFlagClient;

  public ActorDefinitionVersionHelper(final ConfigRepository configRepository,
                                      final DefinitionVersionOverrideProvider overrideProvider,
                                      final FeatureFlagClient featureFlagClient) {
    this.overrideProvider = overrideProvider;
    this.featureFlagClient = featureFlagClient;
    this.configRepository = configRepository;
    LOGGER.info("ActorDefinitionVersionHelper initialized with override provider: {}", overrideProvider.getClass().getSimpleName());
  }

  private ActorDefinitionVersion getDefaultSourceVersion(final StandardSourceDefinition sourceDefinition,
                                                         final UUID workspaceId,
                                                         @Nullable final UUID sourceId)
      throws IOException, ConfigNotFoundException, JsonValidationException {

    final UUID versionId;
    if (sourceId != null && featureFlagClient.boolVariation(UseActorScopedDefaultVersions.INSTANCE, new Workspace(workspaceId))) {
      final SourceConnection source = configRepository.getSourceConnection(sourceId);
      versionId = source.getDefaultVersionId();
    } else {
      versionId = sourceDefinition.getDefaultVersionId();
    }

    if (versionId == null) {
      throw new RuntimeException(String.format("Default version for source is not set (Definition ID: %s, Source ID: %s)",
          sourceDefinition.getSourceDefinitionId(), sourceId));
    }

    return configRepository.getActorDefinitionVersion(versionId);
  }

  private ActorDefinitionVersion getDefaultDestinationVersion(final StandardDestinationDefinition destinationDefinition,
                                                              final UUID workspaceId,
                                                              @Nullable final UUID destinationId)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final UUID versionId;
    if (destinationId != null && featureFlagClient.boolVariation(UseActorScopedDefaultVersions.INSTANCE, new Workspace(workspaceId))) {
      final DestinationConnection destination = configRepository.getDestinationConnection(destinationId);
      versionId = destination.getDefaultVersionId();
    } else {
      versionId = destinationDefinition.getDefaultVersionId();
    }

    if (versionId == null) {
      throw new RuntimeException(String.format("Default version for destination is not set (Definition ID: %s, Destination ID: %s)",
          destinationDefinition.getDestinationDefinitionId(), destinationId));
    }

    return configRepository.getActorDefinitionVersion(versionId);
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
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final ActorDefinitionVersion defaultVersion = getDefaultSourceVersion(sourceDefinition, workspaceId, actorId);

    final Optional<ActorDefinitionVersion> versionOverride = overrideProvider.getOverride(
        ActorType.SOURCE,
        sourceDefinition.getSourceDefinitionId(),
        workspaceId,
        actorId,
        defaultVersion);

    return new ActorDefinitionVersionWithOverrideStatus(versionOverride.orElse(defaultVersion), versionOverride.isPresent());
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
      throws ConfigNotFoundException, IOException, JsonValidationException {
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
      throws ConfigNotFoundException, IOException, JsonValidationException {
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
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final ActorDefinitionVersion defaultVersion = getDefaultDestinationVersion(destinationDefinition, workspaceId, actorId);

    final Optional<ActorDefinitionVersion> versionOverride = overrideProvider.getOverride(
        ActorType.DESTINATION,
        destinationDefinition.getDestinationDefinitionId(),
        workspaceId,
        actorId,
        defaultVersion);

    return new ActorDefinitionVersionWithOverrideStatus(versionOverride.orElse(defaultVersion), versionOverride.isPresent());
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
      throws ConfigNotFoundException, IOException, JsonValidationException {
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
      throws ConfigNotFoundException, IOException, JsonValidationException {
    return getDestinationVersion(destinationDefinition, workspaceId, null);
  }

  /**
   * Helper method to share eligibility logic for free connector program. Checks if either the source
   * or destination is in alpha or beta status.
   *
   * @param workspaceId workspace id
   * @param sourceDefinition source definition
   * @param sourceId source id
   * @param destinationDefinition destination definition
   * @param destinationId destination id
   * @return true if either the source or destination is alpha or beta
   */
  public boolean getSourceOrDestinationIsAlphaOrBeta(final UUID workspaceId,
                                                     final StandardSourceDefinition sourceDefinition,
                                                     final UUID sourceId,
                                                     final StandardDestinationDefinition destinationDefinition,
                                                     final UUID destinationId)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final ActorDefinitionVersion sourceVersion = getSourceVersion(sourceDefinition, workspaceId, sourceId);
    final ActorDefinitionVersion destinationVersion = getDestinationVersion(destinationDefinition, workspaceId, destinationId);

    return hasAlphaOrBetaVersion(List.of(sourceVersion, destinationVersion));
  }

  /**
   * Helper method to retrieve active syncs using a given source version, excluding syncs that have a
   * version override applied.
   *
   * @param sourceDefinition - source definition
   * @param sourceVersionIds - source version ids
   * @return list of pairs, where the first element is the workspace id and the second element is a
   *         list of affected sync ids in the workspace
   */
  public List<Pair<UUID, List<UUID>>> getActiveWorkspaceSyncsWithSourceVersionIds(final StandardSourceDefinition sourceDefinition,
                                                                                  final List<UUID> sourceVersionIds)
      throws IOException, ConfigNotFoundException, JsonValidationException {
    final List<SourceConnection> sourceConnections = configRepository.listSourcesWithVersionIds(sourceVersionIds);
    final Map<UUID, List<SourceConnection>> sourceConnectionsByWorkspace = new HashMap<>();

    // verify that a version override has not been applied to the source, and collect by workspace
    for (final SourceConnection source : sourceConnections) {
      final ActorDefinitionVersionWithOverrideStatus versionWithOverrideStatus =
          getSourceVersionWithOverrideStatus(sourceDefinition, source.getWorkspaceId(), source.getSourceId());
      if (!versionWithOverrideStatus.isOverrideApplied()) {
        sourceConnectionsByWorkspace
            .computeIfAbsent(source.getWorkspaceId(), k -> new ArrayList<>())
            .add(source);
      }
    }

    // get affected syncs for each workspace and add them to the list
    final List<Pair<UUID, List<UUID>>> workspaceSyncIds = new ArrayList<>();
    for (final Map.Entry<UUID, List<SourceConnection>> entry : sourceConnectionsByWorkspace.entrySet()) {
      final UUID workspaceId = entry.getKey();
      final List<SourceConnection> sourcesForWorkspace = entry.getValue();
      final List<UUID> sourceIds = sourcesForWorkspace.stream().map(SourceConnection::getSourceId).toList();
      final StandardSyncQuery syncQuery = new StandardSyncQuery(workspaceId, sourceIds, null, false);
      final List<UUID> activeSyncIds = configRepository.listWorkspaceActiveSyncIds(syncQuery);
      if (!activeSyncIds.isEmpty()) {
        workspaceSyncIds.add(new Pair<>(workspaceId, activeSyncIds));
      }
    }

    return workspaceSyncIds;
  }

  /**
   * Helper method to retrieve active syncs using a given destination version, excluding syncs that
   * have a version override applied.
   *
   * @param destinationDefinition - destination definition
   * @param destinationVersionIds - destination version ids
   * @return list of pairs, where the first element is the workspace id and the second element is a
   *         list of affected sync ids in the workspace
   */
  public List<Pair<UUID, List<UUID>>> getActiveWorkspaceSyncsWithDestinationVersionIds(final StandardDestinationDefinition destinationDefinition,
                                                                                       final List<UUID> destinationVersionIds)
      throws IOException, JsonValidationException, ConfigNotFoundException {
    final List<DestinationConnection> destinationConnections = configRepository.listDestinationsWithVersionIds(destinationVersionIds);
    final Map<UUID, List<DestinationConnection>> destinationConnectionsByWorkspace = new HashMap<>();

    // verify that a version override has not been applied to the destination, and collect by workspace
    for (final DestinationConnection destination : destinationConnections) {
      final ActorDefinitionVersionWithOverrideStatus versionWithOverrideStatus = getDestinationVersionWithOverrideStatus(
          destinationDefinition,
          destination.getWorkspaceId(),
          destination.getDestinationId());
      if (!versionWithOverrideStatus.isOverrideApplied()) {
        destinationConnectionsByWorkspace
            .computeIfAbsent(destination.getWorkspaceId(), k -> new ArrayList<>())
            .add(destination);
      }
    }

    // get affected syncs for each workspace and add them to the list
    final List<Pair<UUID, List<UUID>>> workspaceSyncIds = new ArrayList<>();
    for (final Map.Entry<UUID, List<DestinationConnection>> entry : destinationConnectionsByWorkspace.entrySet()) {
      final UUID workspaceId = entry.getKey();
      final List<DestinationConnection> destinationsForWorkspace = entry.getValue();
      final List<UUID> destinationIds = destinationsForWorkspace.stream().map(DestinationConnection::getDestinationId).toList();
      final StandardSyncQuery syncQuery = new StandardSyncQuery(workspaceId, null, destinationIds, false);
      final List<UUID> activeSyncIds = configRepository.listWorkspaceActiveSyncIds(syncQuery);
      if (!activeSyncIds.isEmpty()) {
        workspaceSyncIds.add(new Pair<>(workspaceId, activeSyncIds));
      }
    }

    return workspaceSyncIds;
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
