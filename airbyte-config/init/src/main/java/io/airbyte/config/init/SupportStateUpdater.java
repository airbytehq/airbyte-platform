/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.init;

import static io.airbyte.featureflag.ContextKt.ANONYMOUS;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorDefinitionVersion.SupportState;
import io.airbyte.config.ActorType;
import io.airbyte.config.Configs.DeploymentMode;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.init.BreakingChangeNotificationHelper.BreakingChangeNotificationData;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.BreakingChangesHelper;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.NotifyBreakingChangesOnSupportStateUpdate;
import io.airbyte.featureflag.Workspace;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import kotlin.Pair;
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

  private final DeploymentMode deploymentMode;
  private final ConfigRepository configRepository;
  private final FeatureFlagClient featureFlagClient;
  private final ActorDefinitionVersionHelper actorDefinitionVersionHelper;
  private final BreakingChangeNotificationHelper breakingChangeNotificationHelper;

  public SupportStateUpdater(final ConfigRepository configRepository,
                             final DeploymentMode deploymentMode,
                             final ActorDefinitionVersionHelper actorDefinitionVersionHelper,
                             final BreakingChangeNotificationHelper breakingChangeNotificationHelper,
                             final FeatureFlagClient featureFlagClient) {
    this.deploymentMode = deploymentMode;
    this.configRepository = configRepository;
    this.featureFlagClient = featureFlagClient;
    this.actorDefinitionVersionHelper = actorDefinitionVersionHelper;
    this.breakingChangeNotificationHelper = breakingChangeNotificationHelper;
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
    final List<ActorDefinitionBreakingChange> applicableBreakingChanges =
        BreakingChangesHelper.filterApplicableBreakingChanges(breakingChangesForDefinition, currentDefaultVersion);

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
    final List<BreakingChangeNotificationData> notificationData = new ArrayList<>();

    for (final StandardSourceDefinition sourceDefinition : sourceDefinitions) {
      final List<ActorDefinitionVersion> actorDefinitionVersions =
          configRepository.listActorDefinitionVersionsForDefinition(sourceDefinition.getSourceDefinitionId());
      final Version currentDefaultVersion = getVersionTag(actorDefinitionVersions, sourceDefinition.getDefaultVersionId());
      final List<ActorDefinitionBreakingChange> breakingChangesForDef =
          breakingChangesMap.getOrDefault(sourceDefinition.getSourceDefinitionId(), List.of());

      final SupportStateUpdate supportStateUpdate =
          getSupportStateUpdate(currentDefaultVersion, referenceDate, breakingChangesForDef, actorDefinitionVersions);
      comboSupportStateUpdate = SupportStateUpdate.merge(comboSupportStateUpdate, supportStateUpdate);

      if (shouldNotifyBreakingChanges() && !supportStateUpdate.deprecatedVersionIds.isEmpty()) {
        final ActorDefinitionBreakingChange latestBreakingChange =
            BreakingChangesHelper.getLastApplicableBreakingChange(configRepository, sourceDefinition.getDefaultVersionId(), breakingChangesForDef);
        notificationData.add(buildSourceNotificationData(
            sourceDefinition,
            latestBreakingChange,
            actorDefinitionVersions,
            supportStateUpdate));
      }
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

      if (shouldNotifyBreakingChanges() && !supportStateUpdate.deprecatedVersionIds.isEmpty()) {
        final ActorDefinitionBreakingChange latestBreakingChange = BreakingChangesHelper.getLastApplicableBreakingChange(configRepository,
            destinationDefinition.getDefaultVersionId(), breakingChangesForDef);
        notificationData.add(buildDestinationNotificationData(
            destinationDefinition,
            latestBreakingChange,
            actorDefinitionVersions,
            supportStateUpdate));
      }
    }

    executeSupportStateUpdate(comboSupportStateUpdate);
    breakingChangeNotificationHelper.notifyDeprecatedSyncs(notificationData);
    log.info("Finished updating support states for all definitions");
  }

  @VisibleForTesting
  BreakingChangeNotificationData buildSourceNotificationData(final StandardSourceDefinition sourceDefinition,
                                                             final ActorDefinitionBreakingChange breakingChange,
                                                             final List<ActorDefinitionVersion> versionsBeforeUpdate,
                                                             final SupportStateUpdate supportStateUpdate)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final List<UUID> newlyDeprecatedVersionIds = getNewlyDeprecatedVersionIds(versionsBeforeUpdate, supportStateUpdate);
    final List<Pair<UUID, List<UUID>>> workspaceSyncIds =
        actorDefinitionVersionHelper.getActiveWorkspaceSyncsWithSourceVersionIds(sourceDefinition, newlyDeprecatedVersionIds);
    final List<UUID> workspaceIds = workspaceSyncIds.stream().map(Pair::getFirst).toList();
    return new BreakingChangeNotificationData(
        ActorType.SOURCE,
        sourceDefinition.getName(),
        workspaceIds,
        breakingChange);
  }

  @VisibleForTesting
  BreakingChangeNotificationData buildDestinationNotificationData(final StandardDestinationDefinition destinationDefinition,
                                                                  final ActorDefinitionBreakingChange breakingChange,
                                                                  final List<ActorDefinitionVersion> versionsBeforeUpdate,
                                                                  final SupportStateUpdate supportStateUpdate)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final List<UUID> newlyDeprecatedVersionIds = getNewlyDeprecatedVersionIds(versionsBeforeUpdate, supportStateUpdate);
    final List<Pair<UUID, List<UUID>>> workspaceSyncIds =
        actorDefinitionVersionHelper.getActiveWorkspaceSyncsWithDestinationVersionIds(destinationDefinition, newlyDeprecatedVersionIds);
    final List<UUID> workspaceIds = workspaceSyncIds.stream().map(Pair::getFirst).toList();
    return new BreakingChangeNotificationData(
        ActorType.DESTINATION,
        destinationDefinition.getName(),
        workspaceIds,
        breakingChange);
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

  private boolean shouldNotifyBreakingChanges() {
    // we only want to notify about these on Cloud
    return deploymentMode == DeploymentMode.CLOUD
        && featureFlagClient.boolVariation(NotifyBreakingChangesOnSupportStateUpdate.INSTANCE, new Workspace(ANONYMOUS));
  }

  /**
   * Gets the version IDs that will go from SUPPORTED to DEPRECATED after applying the
   * SupportStateUpdate. This is used when sending notifications, to ensure we only notify on this
   * specific state transition.
   */
  private List<UUID> getNewlyDeprecatedVersionIds(final List<ActorDefinitionVersion> versionsBeforeUpdate,
                                                  final SupportStateUpdate supportStateUpdate) {
    final List<UUID> previouslySupportedVersionIds =
        versionsBeforeUpdate.stream().filter(v -> v.getSupportState() == SupportState.SUPPORTED).map(ActorDefinitionVersion::getVersionId).toList();
    return supportStateUpdate.deprecatedVersionIds.stream().filter(previouslySupportedVersionIds::contains).toList();
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
