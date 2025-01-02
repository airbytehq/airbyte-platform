/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.config.init

import com.google.common.annotations.VisibleForTesting
import io.airbyte.commons.string.Strings
import io.airbyte.commons.version.Version
import io.airbyte.config.ActorDefinitionBreakingChange
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ActorType
import io.airbyte.config.Configs.DeploymentMode
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.init.BreakingChangeNotificationHelper.BreakingChangeNotificationData
import io.airbyte.config.persistence.BreakingChangesHelper
import io.airbyte.data.exceptions.ConfigNotFoundException
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.featureflag.ANONYMOUS
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.NotifyBreakingChangesOnSupportStateUpdate
import io.airbyte.featureflag.Workspace
import jakarta.inject.Singleton
import org.slf4j.LoggerFactory
import java.io.IOException
import java.time.LocalDate
import java.util.Optional
import java.util.UUID
import java.util.stream.Stream
import kotlin.jvm.optionals.getOrDefault

private const val AUTO_UPGRADE = "auto_upgrade"

/**
 * Updates the support state of actor definition versions according to breaking changes.
 */
@Singleton
class SupportStateUpdater(
  private val actorDefinitionService: ActorDefinitionService,
  private val sourceService: SourceService,
  private val destinationService: DestinationService,
  private val deploymentMode: DeploymentMode,
  private val breakingChangesHelper: BreakingChangesHelper,
  private val breakingChangeNotificationHelper: BreakingChangeNotificationHelper,
  private val featureFlagClient: FeatureFlagClient,
) {
  companion object {
    private val log = LoggerFactory.getLogger(SupportStateUpdater::class.java)
  }

  data class SupportStateUpdate(val unsupportedVersionIds: List<UUID>, val deprecatedVersionIds: List<UUID>, val supportedVersionIds: List<UUID>) {
    companion object {
      /**
       * Returns a new SupportStateUpdate that is the result of merging two given SupportStateUpdates.
       *
       * @param a - the first SupportStateUpdate
       * @param b - the second SupportStateUpdate
       * @return merged SupportStateUpdate
       */
      fun merge(
        a: SupportStateUpdate,
        b: SupportStateUpdate,
      ): SupportStateUpdate {
        return SupportStateUpdate(
          Stream.of(a.unsupportedVersionIds, b.unsupportedVersionIds).flatMap { obj: List<UUID> -> obj.stream() }
            .toList(),
          Stream.of(a.deprecatedVersionIds, b.deprecatedVersionIds).flatMap { obj: List<UUID> -> obj.stream() }
            .toList(),
          Stream.of(a.supportedVersionIds, b.supportedVersionIds).flatMap { obj: List<UUID> -> obj.stream() }
            .toList(),
        )
      }
    }
  }

  /**
   * Calculates a SupportStateUpdate for a given actor definition.
   *
   *
   * The breakingChanges and actorDefinitionVersions are assumed to be for a specific actor
   * definition.
   *
   * @param referenceDate - the date to use as the reference point for calculating support states.
   * Usually this is the current date.
   * @param breakingChangesForDefinition - the breaking changes for a specific actor definition.
   * @param actorDefinitionVersions - the actor definition versions for a specific actor definition.
   * @return SupportStateUpdate
   */
  @VisibleForTesting
  fun getSupportStateUpdate(
    currentDefaultVersion: Version?,
    referenceDate: LocalDate?,
    breakingChangesForDefinition: List<ActorDefinitionBreakingChange?>,
    actorDefinitionVersions: List<ActorDefinitionVersion>,
  ): SupportStateUpdate {
    if (breakingChangesForDefinition.isEmpty()) {
      // we've never had any breaking changes for this actor definition, so everything's supported.
      return SupportStateUpdate(listOf(), listOf(), listOf())
    }

    // Filter for breaking changes that would affect the current default version, in case of rollbacks.
    val applicableBreakingChanges =
      BreakingChangesHelper.filterApplicableBreakingChanges(breakingChangesForDefinition, currentDefaultVersion)

    // Latest stale breaking change is the most recent breaking change for which the upgrade deadline
    // has passed (as of the reference date).
    val latestStaleBreakingChange =
      applicableBreakingChanges.stream()
        .filter { breakingChange: ActorDefinitionBreakingChange -> LocalDate.parse(breakingChange.upgradeDeadline).isBefore(referenceDate) }
        .max(Comparator.comparing { obj: ActorDefinitionBreakingChange -> obj.upgradeDeadline })

    // Latest future breaking change is the most recent breaking change for which the upgrade deadline
    // is still upcoming (as of the reference date).
    val latestFutureBreakingChange =
      applicableBreakingChanges.stream()
        .filter { breakingChange: ActorDefinitionBreakingChange -> LocalDate.parse(breakingChange.upgradeDeadline).isAfter(referenceDate) }
        .max(Comparator.comparing { obj: ActorDefinitionBreakingChange -> obj.upgradeDeadline })
    log.info(
      "CurrentDefaultVersion: {}, LatestStableBreakingChange: {}, LatestFutureBreakingChange: {}",
      currentDefaultVersion?.toString(),
      latestStaleBreakingChange.map { it.version }.getOrDefault("<None>"),
      latestFutureBreakingChange.map { it.version }.getOrDefault("<None>"),
    )

    val versionIdsToUpdateByState =
      mapOf(
        ActorDefinitionVersion.SupportState.UNSUPPORTED to mutableListOf(),
        ActorDefinitionVersion.SupportState.DEPRECATED to mutableListOf(),
        ActorDefinitionVersion.SupportState.SUPPORTED to mutableListOf<UUID>(),
      )

    for (actorDefinitionVersion in actorDefinitionVersions) {
      val version = Version(actorDefinitionVersion.dockerImageTag)
      val supportState = calcVersionSupportState(version, latestStaleBreakingChange, latestFutureBreakingChange)
      if (supportState != actorDefinitionVersion.supportState) {
        versionIdsToUpdateByState[supportState]!!.add(actorDefinitionVersion.versionId)
      }
    }

    return SupportStateUpdate(
      versionIdsToUpdateByState[ActorDefinitionVersion.SupportState.UNSUPPORTED]!!,
      versionIdsToUpdateByState[ActorDefinitionVersion.SupportState.DEPRECATED]!!,
      versionIdsToUpdateByState[ActorDefinitionVersion.SupportState.SUPPORTED]!!,
    )
  }

  /**
   * Updates the version support states for all source and destination definitions.
   */
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun updateSupportStates() {
    updateSupportStates(LocalDate.now())
  }

  /**
   * Updates the version support states for all source and destination definitions based on a
   * reference date, and disables syncs with unsupported versions.
   */
  @VisibleForTesting
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun updateSupportStates(referenceDate: LocalDate?) {
    log.info("Updating support states for all definitions")
    val sourceDefinitions = sourceService.listPublicSourceDefinitions(false)
    val destinationDefinitions = destinationService.listPublicDestinationDefinitions(false)
    val allBreakingChanges = actorDefinitionService.listBreakingChanges()
    val breakingChangesMap = allBreakingChanges.groupBy { it.actorDefinitionId }
    var comboSupportStateUpdate = SupportStateUpdate(listOf(), listOf(), listOf())
    val syncDeprecatedNotificationData: MutableList<BreakingChangeNotificationData> = ArrayList()
    val syncUpcomingAutoUpgradeNotificationData: MutableList<BreakingChangeNotificationData> = ArrayList()

    for (sourceDefinition in sourceDefinitions) {
      log.info("Processing source definition {} {}", sourceDefinition.sourceDefinitionId, sourceDefinition.name)
      val actorDefinitionVersions =
        actorDefinitionService.listActorDefinitionVersionsForDefinition(sourceDefinition.sourceDefinitionId)
      val currentDefaultVersion = getVersionTag(actorDefinitionVersions, sourceDefinition.defaultVersionId)
      val breakingChangesForDef: List<ActorDefinitionBreakingChange> = breakingChangesMap[sourceDefinition.sourceDefinitionId] ?: listOf()
      val supportStateUpdate =
        getSupportStateUpdate(currentDefaultVersion, referenceDate, breakingChangesForDef, actorDefinitionVersions)
      comboSupportStateUpdate = SupportStateUpdate.merge(comboSupportStateUpdate, supportStateUpdate)

      log.info(
        "Supported versions for {} {}: {}",
        sourceDefinition.sourceDefinitionId,
        sourceDefinition.name,
        Strings.join(supportStateUpdate.supportedVersionIds, ","),
      )
      log.info(
        "Deprecated versions for {} {}: {}",
        sourceDefinition.sourceDefinitionId,
        sourceDefinition.name,
        Strings.join(supportStateUpdate.deprecatedVersionIds, ","),
      )
      log.info(
        "Unsupported versions for {} {}: {}",
        sourceDefinition.sourceDefinitionId,
        sourceDefinition.name,
        Strings.join(supportStateUpdate.unsupportedVersionIds, ","),
      )
      if (shouldNotifyBreakingChanges() && supportStateUpdate.deprecatedVersionIds.isNotEmpty()) {
        val latestBreakingChange =
          BreakingChangesHelper.getLastApplicableBreakingChange(
            actorDefinitionService,
            sourceDefinition.defaultVersionId,
            breakingChangesForDef,
          )
        val notificationData =
          buildSourceNotificationData(
            sourceDefinition,
            latestBreakingChange,
            actorDefinitionVersions,
            supportStateUpdate,
          )
        if (AUTO_UPGRADE == latestBreakingChange.deadlineAction) {
          syncUpcomingAutoUpgradeNotificationData.add(notificationData)
        } else {
          syncDeprecatedNotificationData.add(notificationData)
        }
      }
    }

    for (destinationDefinition in destinationDefinitions) {
      val actorDefinitionVersions =
        actorDefinitionService.listActorDefinitionVersionsForDefinition(destinationDefinition.destinationDefinitionId)
      val currentDefaultVersion = getVersionTag(actorDefinitionVersions, destinationDefinition.defaultVersionId)
      val breakingChangesForDef: List<ActorDefinitionBreakingChange> =
        breakingChangesMap[destinationDefinition.destinationDefinitionId] ?: listOf()

      val supportStateUpdate =
        getSupportStateUpdate(currentDefaultVersion, referenceDate, breakingChangesForDef, actorDefinitionVersions)
      comboSupportStateUpdate = SupportStateUpdate.merge(comboSupportStateUpdate, supportStateUpdate)

      if (shouldNotifyBreakingChanges() && supportStateUpdate.deprecatedVersionIds.isNotEmpty()) {
        val latestBreakingChange =
          BreakingChangesHelper.getLastApplicableBreakingChange(
            actorDefinitionService,
            destinationDefinition.defaultVersionId,
            breakingChangesForDef,
          )
        val notificationData =
          buildDestinationNotificationData(
            destinationDefinition,
            latestBreakingChange,
            actorDefinitionVersions,
            supportStateUpdate,
          )
        if (AUTO_UPGRADE == latestBreakingChange.deadlineAction) {
          syncUpcomingAutoUpgradeNotificationData.add(notificationData)
        } else {
          syncDeprecatedNotificationData.add(notificationData)
        }
      }
    }

    executeSupportStateUpdate(comboSupportStateUpdate)
    breakingChangeNotificationHelper.notifyDeprecatedSyncs(syncDeprecatedNotificationData)
    breakingChangeNotificationHelper.notifyUpcomingUpgradeSyncs(syncUpcomingAutoUpgradeNotificationData)
    log.info("Finished updating support states for all definitions")
  }

  @VisibleForTesting
  @Throws(IOException::class)
  fun buildSourceNotificationData(
    sourceDefinition: StandardSourceDefinition,
    breakingChange: ActorDefinitionBreakingChange,
    versionsBeforeUpdate: List<ActorDefinitionVersion>,
    supportStateUpdate: SupportStateUpdate,
  ): BreakingChangeNotificationData {
    val newlyDeprecatedVersionIds = getNewlyDeprecatedVersionIds(versionsBeforeUpdate, supportStateUpdate)
    val workspaceSyncIds =
      breakingChangesHelper.getBreakingActiveSyncsPerWorkspace(
        ActorType.SOURCE,
        sourceDefinition.sourceDefinitionId,
        newlyDeprecatedVersionIds,
      )
    val workspaceIds = workspaceSyncIds.map { it.workspaceId }
    return BreakingChangeNotificationData(
      ActorType.SOURCE,
      sourceDefinition.name,
      workspaceIds,
      breakingChange,
    )
  }

  @VisibleForTesting
  @Throws(IOException::class)
  fun buildDestinationNotificationData(
    destinationDefinition: StandardDestinationDefinition,
    breakingChange: ActorDefinitionBreakingChange,
    versionsBeforeUpdate: List<ActorDefinitionVersion>,
    supportStateUpdate: SupportStateUpdate,
  ): BreakingChangeNotificationData {
    val newlyDeprecatedVersionIds = getNewlyDeprecatedVersionIds(versionsBeforeUpdate, supportStateUpdate)
    val workspaceSyncIds =
      breakingChangesHelper.getBreakingActiveSyncsPerWorkspace(
        ActorType.DESTINATION,
        destinationDefinition.destinationDefinitionId,
        newlyDeprecatedVersionIds,
      )
    val workspaceIds = workspaceSyncIds.map { it.workspaceId }
    return BreakingChangeNotificationData(
      ActorType.DESTINATION,
      destinationDefinition.name,
      workspaceIds,
      breakingChange,
    )
  }

  private fun calcVersionSupportState(
    version: Version,
    latestStaleBreakingChange: Optional<ActorDefinitionBreakingChange>,
    latestFutureBreakingChange: Optional<ActorDefinitionBreakingChange>,
  ): ActorDefinitionVersion.SupportState {
    // A version is UNSUPPORTED if it's older (semver) than a version that has had a breaking change,
    // and the deadline to upgrade from that breaking change has already passed.
    if (latestStaleBreakingChange.isPresent && latestStaleBreakingChange.get().version.greaterThan(version)) {
      return ActorDefinitionVersion.SupportState.UNSUPPORTED
    }

    // A version is DEPRECATED if it's older (semver) than a version that has had a breaking change,
    // and the deadline to upgrade from that breaking change has NOT yet passed.
    if (latestFutureBreakingChange.isPresent && latestFutureBreakingChange.get().version.greaterThan(version)) {
      return ActorDefinitionVersion.SupportState.DEPRECATED
    }

    return ActorDefinitionVersion.SupportState.SUPPORTED
  }

  /**
   * Updates the version support states for a given source definition.
   */
  @Throws(ConfigNotFoundException::class, IOException::class)
  fun updateSupportStatesForSourceDefinition(sourceDefinition: StandardSourceDefinition) {
    if (!sourceDefinition.custom) {
      log.info("Updating support states for source definition: {}", sourceDefinition.name)
      val defaultActorDefinitionVersion =
        actorDefinitionService.getActorDefinitionVersion(sourceDefinition.defaultVersionId)
      val currentDefaultVersion = Version(defaultActorDefinitionVersion.dockerImageTag)
      updateSupportStatesForActorDefinition(sourceDefinition.sourceDefinitionId, currentDefaultVersion)

      log.info("Finished updating support states for source definition: {}", sourceDefinition.name)
    }
  }

  /**
   * Updates the version support states for a given destination definition.
   */
  @Throws(ConfigNotFoundException::class, IOException::class)
  fun updateSupportStatesForDestinationDefinition(destinationDefinition: StandardDestinationDefinition) {
    if (!destinationDefinition.custom) {
      log.info("Updating support states for destination definition: {}", destinationDefinition.name)
      val defaultActorDefinitionVersion =
        actorDefinitionService.getActorDefinitionVersion(destinationDefinition.defaultVersionId)
      val currentDefaultVersion = Version(defaultActorDefinitionVersion.dockerImageTag)
      updateSupportStatesForActorDefinition(destinationDefinition.destinationDefinitionId, currentDefaultVersion)

      log.info("Finished updating support states for destination definition: {}", destinationDefinition.name)
    }
  }

  @Throws(IOException::class)
  private fun updateSupportStatesForActorDefinition(
    actorDefinitionId: UUID,
    currentDefaultVersion: Version,
  ) {
    val breakingChanges = actorDefinitionService.listBreakingChangesForActorDefinition(actorDefinitionId)
    val actorDefinitionVersions = actorDefinitionService.listActorDefinitionVersionsForDefinition(actorDefinitionId)
    val supportStateUpdate =
      getSupportStateUpdate(currentDefaultVersion, LocalDate.now(), breakingChanges, actorDefinitionVersions)
    executeSupportStateUpdate(supportStateUpdate)
  }

  private fun getVersionTag(
    actorDefinitionVersions: List<ActorDefinitionVersion>,
    versionId: UUID,
  ): Version {
    return actorDefinitionVersions.stream()
      .filter { actorDefinitionVersion: ActorDefinitionVersion -> actorDefinitionVersion.versionId == versionId }
      .findFirst()
      .map { actorDefinitionVersion: ActorDefinitionVersion -> Version(actorDefinitionVersion.dockerImageTag) }
      .orElseThrow()
  }

  private fun shouldNotifyBreakingChanges(): Boolean {
    // we only want to notify about these on Cloud
    return (
      deploymentMode == DeploymentMode.CLOUD &&
        featureFlagClient.boolVariation(NotifyBreakingChangesOnSupportStateUpdate, Workspace(ANONYMOUS))
    )
  }

  /**
   * Gets the version IDs that will go from SUPPORTED to DEPRECATED after applying the
   * SupportStateUpdate. This is used when sending notifications, to ensure we only notify on this
   * specific state transition.
   */
  private fun getNewlyDeprecatedVersionIds(
    versionsBeforeUpdate: List<ActorDefinitionVersion>,
    supportStateUpdate: SupportStateUpdate,
  ): List<UUID> {
    val previouslySupportedVersionIds =
      versionsBeforeUpdate.stream().filter { v: ActorDefinitionVersion -> v.supportState == ActorDefinitionVersion.SupportState.SUPPORTED }
        .map { obj: ActorDefinitionVersion -> obj.versionId }.toList()
    return supportStateUpdate.deprecatedVersionIds.stream().filter { o: UUID -> previouslySupportedVersionIds.contains(o) }.toList()
  }

  /**
   * Processes a given SupportStateUpdate by updating the support states in the db.
   *
   * @param supportStateUpdate - the SupportStateUpdate to process.
   */
  @Throws(IOException::class)
  private fun executeSupportStateUpdate(supportStateUpdate: SupportStateUpdate) {
    if (supportStateUpdate.unsupportedVersionIds.isNotEmpty()) {
      actorDefinitionService.setActorDefinitionVersionSupportStates(
        supportStateUpdate.unsupportedVersionIds,
        ActorDefinitionVersion.SupportState.UNSUPPORTED,
      )
    }

    if (supportStateUpdate.deprecatedVersionIds.isNotEmpty()) {
      actorDefinitionService.setActorDefinitionVersionSupportStates(
        supportStateUpdate.deprecatedVersionIds,
        ActorDefinitionVersion.SupportState.DEPRECATED,
      )
    }

    if (supportStateUpdate.supportedVersionIds.isNotEmpty()) {
      actorDefinitionService.setActorDefinitionVersionSupportStates(
        supportStateUpdate.supportedVersionIds,
        ActorDefinitionVersion.SupportState.SUPPORTED,
      )
    }
  }
}
