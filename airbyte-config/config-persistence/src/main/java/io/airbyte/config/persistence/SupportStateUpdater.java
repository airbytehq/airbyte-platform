/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorDefinitionVersion.SupportState;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
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

  public SupportStateUpdater(final ConfigRepository configRepository) {
    this.configRepository = configRepository;
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
  public void updateSupportStatesForSourceDefinition(final StandardSourceDefinition sourceDefinition) throws ConfigNotFoundException, IOException {
    if (!sourceDefinition.getCustom()) {
      final ActorDefinitionVersion defaultActorDefinitionVersion = configRepository.getActorDefinitionVersion(sourceDefinition.getDefaultVersionId());
      final Version currentDefaultVersion = new Version(defaultActorDefinitionVersion.getDockerImageTag());
      updateSupportStatesForActorDefinition(sourceDefinition.getSourceDefinitionId(), currentDefaultVersion);
    }
  }

  /**
   * Updates the version support states for a given destination definition.
   */
  public void updateSupportStatesForDestinationDefinition(final StandardDestinationDefinition destinationDefinition)
      throws ConfigNotFoundException, IOException {
    if (!destinationDefinition.getCustom()) {
      final ActorDefinitionVersion defaultActorDefinitionVersion =
          configRepository.getActorDefinitionVersion(destinationDefinition.getDefaultVersionId());
      final Version currentDefaultVersion = new Version(defaultActorDefinitionVersion.getDockerImageTag());
      updateSupportStatesForActorDefinition(destinationDefinition.getDestinationDefinitionId(), currentDefaultVersion);
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
   * Updates the version support states for all source and destination definitions.
   */
  public void updateSupportStates() throws IOException {
    final List<StandardSourceDefinition> sourceDefinitions = configRepository.listPublicSourceDefinitions(false);
    final List<StandardDestinationDefinition> destinationDefinitions = configRepository.listPublicDestinationDefinitions(false);
    final List<ActorDefinitionBreakingChange> breakingChanges = configRepository.listBreakingChanges();

    SupportStateUpdate comboSupportStateUpdate = new SupportStateUpdate(List.of(), List.of(), List.of());

    for (final StandardSourceDefinition sourceDefinition : sourceDefinitions) {
      final List<ActorDefinitionVersion> actorDefinitionVersions =
          configRepository.listActorDefinitionVersionsForDefinition(sourceDefinition.getSourceDefinitionId());
      final Version currentDefaultVersion = getVersionTag(actorDefinitionVersions, sourceDefinition.getDefaultVersionId());

      final SupportStateUpdate supportStateUpdate =
          getSupportStateUpdate(currentDefaultVersion, LocalDate.now(), breakingChanges, actorDefinitionVersions);

      comboSupportStateUpdate = SupportStateUpdate.merge(comboSupportStateUpdate, supportStateUpdate);
    }

    for (final StandardDestinationDefinition destinationDefinition : destinationDefinitions) {
      final List<ActorDefinitionVersion> actorDefinitionVersions =
          configRepository.listActorDefinitionVersionsForDefinition(destinationDefinition.getDestinationDefinitionId());
      final Version currentDefaultVersion = getVersionTag(actorDefinitionVersions, destinationDefinition.getDefaultVersionId());

      final SupportStateUpdate supportStateUpdate =
          getSupportStateUpdate(currentDefaultVersion, LocalDate.now(), breakingChanges, actorDefinitionVersions);

      comboSupportStateUpdate = SupportStateUpdate.merge(comboSupportStateUpdate, supportStateUpdate);
    }

    executeSupportStateUpdate(comboSupportStateUpdate);
  }

  /**
   * Processes a given SupportStateUpdate by updating the support states in the db.
   *
   * @param supportStateUpdate - the SupportStateUpdate to process.
   */
  private void executeSupportStateUpdate(final SupportStateUpdate supportStateUpdate) throws IOException {
    // TODO(pedro): This is likely where we disable syncs for the now-unsupported versions.

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
