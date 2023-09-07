/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static io.airbyte.featureflag.ContextKt.ANONYMOUS;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorDefinitionVersion.SupportState;
import io.airbyte.config.Configs.DeploymentMode;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSync.Status;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper.ActorDefinitionVersionWithOverrideStatus;
import io.airbyte.config.persistence.ConfigRepository.StandardSyncQuery;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.PauseSyncsWithUnsupportedActors;
import io.airbyte.featureflag.Workspace;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

/**
 * Updates the support state of actor definition versions according to breaking changes.
 */
@Singleton
@Slf4j
public class SupportStateUpdater {

  record SupportStateUpdate(List<UUID> unsupportedVersionIds, List<UUID> deprecatedVersionIds, List<UUID> supportedVersionIds) {

    /**
     * Returns a new SupportStateUpdate that is the result of merging two given SupportStateUpdates.
     *
     * @param a - the first SupportStateUpdate
     * @param b - the second SupportStateUpdate
     * @return merged SupportStateUpdate
     */
    public static SupportStateUpdate merge(final SupportStateUpdate a, final SupportStateUpdate b) {
      return new SupportStateUpdate(
          Stream.of(a.unsupportedVersionIds, b.unsupportedVersionIds).flatMap(List::stream).toList(),
          Stream.of(a.deprecatedVersionIds, b.deprecatedVersionIds).flatMap(List::stream).toList(),
          Stream.of(a.supportedVersionIds, b.supportedVersionIds).flatMap(List::stream).toList());
    }

  }

  private final ConfigRepository configRepository;
  private final ActorDefinitionVersionHelper actorDefinitionVersionHelper;
  private final DeploymentMode deploymentMode;
  private final FeatureFlagClient featureFlagClient;

  public SupportStateUpdater(final ConfigRepository configRepository,
                             final ActorDefinitionVersionHelper actorDefinitionVersionHelper,
                             final DeploymentMode deploymentMode,
                             final FeatureFlagClient featureFlagClient) {
    this.configRepository = configRepository;
    this.actorDefinitionVersionHelper = actorDefinitionVersionHelper;
    this.deploymentMode = deploymentMode;
    this.featureFlagClient = featureFlagClient;
  }

  @VisibleForTesting
  boolean shouldDisableSyncs() {
    if (!featureFlagClient.boolVariation(PauseSyncsWithUnsupportedActors.INSTANCE, new Workspace(ANONYMOUS))) {
      return false;
    }

    // We only disable syncs on Cloud. OSS users can continue to run on unsupported versions.
    return deploymentMode == DeploymentMode.CLOUD;
  }

  /**
   * Calculates a SupportStateUpdate for a given actor definition.
   * <p>
   * The breakingChanges and actorDefinitionVersions are assumed to be for a specific actor
   * definition.
   *
   * @param referenceDate - the date to use as the reference point for calculating support states.
   *        Usually this is the current date.
   * @param breakingChangesForDefinition - the breaking changes for a specific actor definition.
   * @param actorDefinitionVersions - the actor definition versions for a specific actor definition.
   * @return SupportStateUpdate
   */
  @VisibleForTesting
  SupportStateUpdate getSupportStateUpdate(final Version currentDefaultVersion,
                                           final LocalDate referenceDate,
                                           final List<ActorDefinitionBreakingChange> breakingChangesForDefinition,
                                           final List<ActorDefinitionVersion> actorDefinitionVersions) {
    if (breakingChangesForDefinition.isEmpty()) {
      // we've never had any breaking changes for this actor definition, so everything's supported.
      return new SupportStateUpdate(List.of(), List.of(), List.of());
    }

    // Filter for breaking changes that would affect the current default version, in case of rollbacks.
    final List<ActorDefinitionBreakingChange> applicableBreakingChanges = breakingChangesForDefinition.stream()
        .filter(breakingChange -> currentDefaultVersion.greaterThanOrEqualTo(breakingChange.getVersion()))
        .toList();

    // Latest stale breaking change is the most recent breaking change for which the upgrade deadline
    // has passed (as of the reference date).
    final Optional<ActorDefinitionBreakingChange> latestStaleBreakingChange = applicableBreakingChanges.stream()
        .filter(breakingChange -> LocalDate.parse(breakingChange.getUpgradeDeadline()).isBefore(referenceDate))
        .max(Comparator.comparing(ActorDefinitionBreakingChange::getUpgradeDeadline));

    // Latest future breaking change is the most recent breaking change for which the upgrade deadline
    // is still upcoming (as of the reference date).
    final Optional<ActorDefinitionBreakingChange> latestFutureBreakingChange = applicableBreakingChanges.stream()
        .filter(breakingChange -> LocalDate.parse(breakingChange.getUpgradeDeadline()).isAfter(referenceDate))
        .max(Comparator.comparing(ActorDefinitionBreakingChange::getUpgradeDeadline));

    final Map<SupportState, List<UUID>> versionIdsToUpdateByState = Map.of(
        SupportState.UNSUPPORTED, new ArrayList<>(),
        SupportState.DEPRECATED, new ArrayList<>(),
        SupportState.SUPPORTED, new ArrayList<>());

    for (final ActorDefinitionVersion actorDefinitionVersion : actorDefinitionVersions) {
      final Version version = new Version(actorDefinitionVersion.getDockerImageTag());
      final SupportState supportState = calcVersionSupportState(version, latestStaleBreakingChange, latestFutureBreakingChange);
      if (supportState != actorDefinitionVersion.getSupportState()) {
        versionIdsToUpdateByState.get(supportState).add(actorDefinitionVersion.getVersionId());
      }
    }

    return new SupportStateUpdate(
        versionIdsToUpdateByState.get(SupportState.UNSUPPORTED),
        versionIdsToUpdateByState.get(SupportState.DEPRECATED),
        versionIdsToUpdateByState.get(SupportState.SUPPORTED));
  }

  private SupportState calcVersionSupportState(final Version version,
                                               final Optional<ActorDefinitionBreakingChange> latestStaleBreakingChange,
                                               final Optional<ActorDefinitionBreakingChange> latestFutureBreakingChange) {
    // A version is UNSUPPORTED if it's older (semver) than a version that has had a breaking change,
    // and the deadline to upgrade from that breaking change has already passed.
    if (latestStaleBreakingChange.isPresent() && latestStaleBreakingChange.get().getVersion().greaterThan(version)) {
      return SupportState.UNSUPPORTED;
    }

    // A version is DEPRECATED if it's older (semver) than a version that has had a breaking change,
    // and the deadline to upgrade from that breaking change has NOT yet passed.
    if (latestFutureBreakingChange.isPresent() && latestFutureBreakingChange.get().getVersion().greaterThan(version)) {
      return SupportState.DEPRECATED;
    }

    return SupportState.SUPPORTED;
  }

  /**
   * Updates the version support states for a given source definition.
   */
  public void updateSupportStatesForSourceDefinition(final StandardSourceDefinition sourceDefinition)
      throws ConfigNotFoundException, IOException {
    if (!sourceDefinition.getCustom()) {
      log.info("Updating support states for source definition: {}", sourceDefinition.getName());
      final ActorDefinitionVersion defaultActorDefinitionVersion = configRepository.getActorDefinitionVersion(sourceDefinition.getDefaultVersionId());
      final Version currentDefaultVersion = new Version(defaultActorDefinitionVersion.getDockerImageTag());
      updateSupportStatesForActorDefinition(sourceDefinition.getSourceDefinitionId(), currentDefaultVersion);

      log.info("Finished updating support states for source definition: {}", sourceDefinition.getName());
    }
  }

  /**
   * Updates the version support states for a given destination definition.
   */
  public void updateSupportStatesForDestinationDefinition(final StandardDestinationDefinition destinationDefinition)
      throws ConfigNotFoundException, IOException {
    if (!destinationDefinition.getCustom()) {
      log.info("Updating support states for destination definition: {}", destinationDefinition.getName());
      final ActorDefinitionVersion defaultActorDefinitionVersion =
          configRepository.getActorDefinitionVersion(destinationDefinition.getDefaultVersionId());
      final Version currentDefaultVersion = new Version(defaultActorDefinitionVersion.getDockerImageTag());
      updateSupportStatesForActorDefinition(destinationDefinition.getDestinationDefinitionId(), currentDefaultVersion);

      log.info("Finished updating support states for destination definition: {}", destinationDefinition.getName());
    }
  }

  private void updateSupportStatesForActorDefinition(final UUID actorDefinitionId, final Version currentDefaultVersion) throws IOException {
    final List<ActorDefinitionBreakingChange> breakingChanges = configRepository.listBreakingChangesForActorDefinition(actorDefinitionId);
    final List<ActorDefinitionVersion> actorDefinitionVersions = configRepository.listActorDefinitionVersionsForDefinition(actorDefinitionId);
    final SupportStateUpdate supportStateUpdate =
        getSupportStateUpdate(currentDefaultVersion, LocalDate.now(), breakingChanges, actorDefinitionVersions);
    executeSupportStateUpdate(supportStateUpdate);
  }

  private Version getVersionTag(final List<ActorDefinitionVersion> actorDefinitionVersions, final UUID versionId) {
    return actorDefinitionVersions.stream()
        .filter(actorDefinitionVersion -> actorDefinitionVersion.getVersionId().equals(versionId))
        .findFirst()
        .map(actorDefinitionVersion -> new Version(actorDefinitionVersion.getDockerImageTag()))
        .orElseThrow();
  }

  /**
   * Calculates unsupported version IDs after the support state update would be applied.
   * <p>
   * This is done by taking the set of all previously unsupported versions, adding newly unsupported
   * versions, and removing any versions that would be supported or deprecated after the update.
   *
   * @param versionsBeforeUpdate - actor definition versions before applying the SupportStateUpdate
   * @param supportStateUpdate - the SupportStateUpdate that would be applied
   * @return - unsupported actor definition version IDs
   */
  @VisibleForTesting
  List<UUID> getUnsupportedVersionIdsAfterUpdate(final List<ActorDefinitionVersion> versionsBeforeUpdate,
                                                 final SupportStateUpdate supportStateUpdate) {
    final List<UUID> prevUnsupportedVersionIds = versionsBeforeUpdate.stream()
        .filter(v -> v.getSupportState() == SupportState.UNSUPPORTED)
        .map(ActorDefinitionVersion::getVersionId)
        .filter(verId -> !supportStateUpdate.supportedVersionIds().contains(verId) && !supportStateUpdate.deprecatedVersionIds().contains(verId))
        .toList();
    return Stream.of(prevUnsupportedVersionIds, supportStateUpdate.unsupportedVersionIds())
        .flatMap(Collection::stream)
        .toList();
  }

  /**
   * Updates the version support states for all source and destination definitions.
   */
  public void updateSupportStates() throws IOException, JsonValidationException, ConfigNotFoundException {
    updateSupportStates(LocalDate.now());
  }

  /**
   * Updates the version support states for all source and destination definitions based on a
   * reference date, and disables syncs with unsupported versions.
   */
  @VisibleForTesting
  void updateSupportStates(final LocalDate referenceDate) throws IOException, JsonValidationException, ConfigNotFoundException {
    log.info("Updating support states for all definitions");
    final List<StandardSourceDefinition> sourceDefinitions = configRepository.listPublicSourceDefinitions(false);
    final List<StandardDestinationDefinition> destinationDefinitions = configRepository.listPublicDestinationDefinitions(false);
    final List<ActorDefinitionBreakingChange> allBreakingChanges = configRepository.listBreakingChanges();
    final Map<UUID, List<ActorDefinitionBreakingChange>> breakingChangesMap = allBreakingChanges.stream()
        .collect(Collectors.groupingBy(ActorDefinitionBreakingChange::getActorDefinitionId));

    SupportStateUpdate comboSupportStateUpdate = new SupportStateUpdate(List.of(), List.of(), List.of());
    final List<StandardSync> syncsToDisable = new ArrayList<>();

    for (final StandardSourceDefinition sourceDefinition : sourceDefinitions) {
      final List<ActorDefinitionVersion> actorDefinitionVersions =
          configRepository.listActorDefinitionVersionsForDefinition(sourceDefinition.getSourceDefinitionId());
      final Version currentDefaultVersion = getVersionTag(actorDefinitionVersions, sourceDefinition.getDefaultVersionId());
      final List<ActorDefinitionBreakingChange> breakingChangesForDef =
          breakingChangesMap.getOrDefault(sourceDefinition.getSourceDefinitionId(), List.of());

      final SupportStateUpdate supportStateUpdate =
          getSupportStateUpdate(currentDefaultVersion, referenceDate, breakingChangesForDef, actorDefinitionVersions);
      comboSupportStateUpdate = SupportStateUpdate.merge(comboSupportStateUpdate, supportStateUpdate);

      final List<UUID> unsupportedVersionIds = getUnsupportedVersionIdsAfterUpdate(actorDefinitionVersions, supportStateUpdate);
      final List<StandardSync> syncsToDisableForSource = getSyncsToDisableForSource(sourceDefinition, unsupportedVersionIds);
      syncsToDisable.addAll(syncsToDisableForSource);
    }

    for (final StandardDestinationDefinition destinationDefinition : destinationDefinitions) {
      final List<ActorDefinitionVersion> actorDefinitionVersions =
          configRepository.listActorDefinitionVersionsForDefinition(destinationDefinition.getDestinationDefinitionId());
      final Version currentDefaultVersion = getVersionTag(actorDefinitionVersions, destinationDefinition.getDefaultVersionId());
      final List<ActorDefinitionBreakingChange> breakingChangesForDef =
          breakingChangesMap.getOrDefault(destinationDefinition.getDestinationDefinitionId(), List.of());

      final SupportStateUpdate supportStateUpdate =
          getSupportStateUpdate(currentDefaultVersion, referenceDate, breakingChangesForDef, actorDefinitionVersions);
      comboSupportStateUpdate = SupportStateUpdate.merge(comboSupportStateUpdate, supportStateUpdate);

      final List<UUID> unsupportedVersionIds = getUnsupportedVersionIdsAfterUpdate(actorDefinitionVersions, supportStateUpdate);
      final List<StandardSync> syncsToDisableForDestination = getSyncsToDisableForDestination(destinationDefinition, unsupportedVersionIds);
      syncsToDisable.addAll(syncsToDisableForDestination);
    }

    executeSupportStateUpdate(comboSupportStateUpdate);
    disableSyncs(syncsToDisable);
    log.info("Finished updating support states for all definitions");
  }

  @VisibleForTesting
  List<StandardSync> getSyncsToDisableForSource(final StandardSourceDefinition sourceDefinition, final List<UUID> unsupportedVersionIds)
      throws IOException, JsonValidationException, ConfigNotFoundException {
    if (!shouldDisableSyncs() || unsupportedVersionIds.isEmpty()) {
      return Collections.emptyList();
    }

    final List<StandardSync> syncsToDisable = new ArrayList<>();
    final List<SourceConnection> sourceConnections = configRepository.listSourcesWithVersionIds(unsupportedVersionIds);
    final Map<UUID, List<SourceConnection>> sourceConnectionsByWorkspace = new HashMap<>();

    // verify that a version override has not been applied to the source, and collect by workspace
    for (final SourceConnection source : sourceConnections) {
      final ActorDefinitionVersionWithOverrideStatus versionWithOverrideStatus =
          actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(sourceDefinition, source.getWorkspaceId(), source.getSourceId());
      if (!versionWithOverrideStatus.isOverrideApplied()) {
        sourceConnectionsByWorkspace
            .computeIfAbsent(source.getWorkspaceId(), k -> new ArrayList<>())
            .add(source);
      }
    }

    // get affected syncs for each workspace and add them to the list
    for (final Map.Entry<UUID, List<SourceConnection>> entry : sourceConnectionsByWorkspace.entrySet()) {
      final UUID workspaceId = entry.getKey();
      final List<SourceConnection> sourcesForWorkspace = entry.getValue();
      final List<UUID> sourceIds = sourcesForWorkspace.stream().map(SourceConnection::getSourceId).toList();
      final StandardSyncQuery syncQuery = new StandardSyncQuery(workspaceId, sourceIds, null, false);
      final List<StandardSync> standardSyncs = configRepository.listWorkspaceStandardSyncs(syncQuery);
      syncsToDisable.addAll(standardSyncs);
    }

    return syncsToDisable;
  }

  @VisibleForTesting
  List<StandardSync> getSyncsToDisableForDestination(final StandardDestinationDefinition destinationDefinition,
                                                     final List<UUID> unsupportedVersionIds)
      throws IOException, JsonValidationException, ConfigNotFoundException {
    if (!shouldDisableSyncs() || unsupportedVersionIds.isEmpty()) {
      return Collections.emptyList();
    }

    final List<StandardSync> syncsToDisable = new ArrayList<>();
    final List<DestinationConnection> destinationConnections = configRepository.listDestinationsWithVersionIds(unsupportedVersionIds);
    final Map<UUID, List<DestinationConnection>> destinationConnectionsByWorkspace = new HashMap<>();

    // verify that a version override has not been applied to the destination, and collect by workspace
    for (final DestinationConnection destination : destinationConnections) {
      final ActorDefinitionVersionWithOverrideStatus versionWithOverrideStatus =
          actorDefinitionVersionHelper.getDestinationVersionWithOverrideStatus(
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
    for (final Map.Entry<UUID, List<DestinationConnection>> entry : destinationConnectionsByWorkspace.entrySet()) {
      final UUID workspaceId = entry.getKey();
      final List<DestinationConnection> destinationsForWorkspace = entry.getValue();
      final List<UUID> destinationIds = destinationsForWorkspace.stream().map(DestinationConnection::getDestinationId).toList();
      final StandardSyncQuery syncQuery = new StandardSyncQuery(workspaceId, null, destinationIds, false);
      final List<StandardSync> standardSyncs = configRepository.listWorkspaceStandardSyncs(syncQuery);
      syncsToDisable.addAll(standardSyncs);
    }

    return syncsToDisable;
  }

  @VisibleForTesting
  void disableSyncs(final List<StandardSync> syncsToDisable) throws IOException {
    final List<StandardSync> activeSyncs = syncsToDisable.stream()
        .filter(s -> s.getStatus() == Status.ACTIVE)
        .toList();

    for (final StandardSync sync : activeSyncs) {
      configRepository.writeStandardSync(sync.withStatus(Status.INACTIVE));
    }

    if (!activeSyncs.isEmpty()) {
      log.info("Disabled {} syncs with unsupported versions", activeSyncs.size());
    }
  }

  /**
   * Processes a given SupportStateUpdate by updating the support states in the db.
   *
   * @param supportStateUpdate - the SupportStateUpdate to process.
   */
  private void executeSupportStateUpdate(final SupportStateUpdate supportStateUpdate) throws IOException {
    if (!supportStateUpdate.unsupportedVersionIds.isEmpty()) {
      configRepository.setActorDefinitionVersionSupportStates(supportStateUpdate.unsupportedVersionIds,
          ActorDefinitionVersion.SupportState.UNSUPPORTED);
    }

    if (!supportStateUpdate.deprecatedVersionIds.isEmpty()) {
      configRepository.setActorDefinitionVersionSupportStates(supportStateUpdate.deprecatedVersionIds,
          ActorDefinitionVersion.SupportState.DEPRECATED);
    }

    if (!supportStateUpdate.supportedVersionIds.isEmpty()) {
      configRepository.setActorDefinitionVersionSupportStates(supportStateUpdate.supportedVersionIds,
          ActorDefinitionVersion.SupportState.SUPPORTED);
    }
  }

}
