package io.airbyte.data.helpers

import com.google.common.annotations.VisibleForTesting
import io.airbyte.commons.version.Version
import io.airbyte.config.ActorDefinitionBreakingChange
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ActorType
import io.airbyte.config.BreakingChangeScope
import io.airbyte.config.ConfigOriginType
import io.airbyte.config.ConfigResourceType
import io.airbyte.config.ConfigScopeType
import io.airbyte.config.DestinationConnection
import io.airbyte.config.ScopedConfiguration
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.helpers.BreakingChangeScopeFactory
import io.airbyte.config.helpers.StreamBreakingChangeScope
import io.airbyte.data.exceptions.InvalidRequestException
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.ConnectionTimelineEventService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.shared.ConfigScopeMapWithId
import io.airbyte.data.services.shared.ConnectorUpdate
import io.airbyte.data.services.shared.ConnectorVersionKey
import io.airbyte.featureflag.ANONYMOUS
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.UseBreakingChangeScopes
import io.airbyte.featureflag.Workspace
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.io.IOException
import java.util.UUID
import java.util.stream.Collectors

private val logger = KotlinLogging.logger {}

@Singleton
class ActorDefinitionVersionUpdater(
  private val featureFlagClient: FeatureFlagClient,
  private val connectionService: ConnectionService,
  private val actorDefinitionService: ActorDefinitionService,
  private val scopedConfigurationService: ScopedConfigurationService,
  private val connectionTimelineEventService: ConnectionTimelineEventService,
) {
  fun updateDestinationDefaultVersion(
    destinationDefinition: StandardDestinationDefinition,
    newDefaultVersion: ActorDefinitionVersion,
    breakingChangesForDefinition: List<ActorDefinitionBreakingChange>,
  ) {
    return updateDefaultVersion(
      destinationDefinition.destinationDefinitionId,
      newDefaultVersion,
      breakingChangesForDefinition,
    )
  }

  fun updateSourceDefaultVersion(
    sourceDefinition: StandardSourceDefinition,
    newDefaultVersion: ActorDefinitionVersion,
    breakingChangesForDefinition: List<ActorDefinitionBreakingChange>,
  ) {
    return updateDefaultVersion(
      sourceDefinition.sourceDefinitionId,
      newDefaultVersion,
      breakingChangesForDefinition,
    )
  }

  /**
   * Upgrade the source to the latest version, opting-in to any breaking changes that may exist.
   */
  fun upgradeActorVersion(
    source: SourceConnection,
    sourceDefinition: StandardSourceDefinition,
  ) {
    return upgradeActorVersion(
      source.sourceId,
      sourceDefinition.sourceDefinitionId,
      ActorType.SOURCE,
      source.name,
    )
  }

  /**
   * Upgrade the destination to the latest version, opting-in to any breaking changes that may exist.
   */
  fun upgradeActorVersion(
    destination: DestinationConnection,
    destinationDefinition: StandardDestinationDefinition,
  ) {
    return upgradeActorVersion(
      destination.destinationId,
      destinationDefinition.destinationDefinitionId,
      ActorType.DESTINATION,
      destination.name,
    )
  }

  @VisibleForTesting
  internal fun upgradeActorVersion(
    actorId: UUID,
    actorDefinitionId: UUID,
    actorType: ActorType,
    actorName: String,
  ) {
    val versionPinConfigOpt =
      scopedConfigurationService.getScopedConfiguration(
        ConnectorVersionKey.key,
        ConfigResourceType.ACTOR_DEFINITION,
        actorDefinitionId,
        ConfigScopeType.ACTOR,
        actorId,
      )

    versionPinConfigOpt.ifPresent { versionPinConfig ->
      if (versionPinConfig.originType != ConfigOriginType.BREAKING_CHANGE) {
        throw IllegalStateException("This %s is manually pinned to a version, and therefore cannot be upgraded.".format(actorType))
      }

      scopedConfigurationService.deleteScopedConfiguration(versionPinConfig.id)
      try {
        val previousVersion = actorDefinitionService.getActorDefinitionVersion(UUID.fromString(versionPinConfig.value)).dockerImageTag
        val newVersion = actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(actorDefinitionId).get().dockerImageTag
        val connections =
          if (actorType == ActorType.SOURCE) {
            connectionService.listConnectionsBySource(actorId, true)
          } else {
            connectionService.listConnectionsByDestination(actorId, true)
          }
        connections
          .forEach {
            connectionTimelineEventService.writeEvent(
              it.connectionId,
              ConnectorUpdate(
                previousVersion,
                newVersion,
                ConnectorUpdate.ConnectorType.SOURCE,
                actorName,
                ConnectorUpdate.UpdateType.BREAKING_CHANGE_MANUAL.name,
              ),
              null,
            )
          }
      } catch (e: Exception) {
        logger.error(e) { "Failed to write connector upgrade timeline event for actor $actorDefinitionId: $e" }
      }
    }
  }

  @VisibleForTesting
  internal fun updateDefaultVersion(
    actorDefinitionId: UUID,
    newDefaultVersion: ActorDefinitionVersion,
    breakingChangesForDefinition: List<ActorDefinitionBreakingChange>,
  ) {
    if (newDefaultVersion.versionId == null) {
      throw RuntimeException("Can't set an actorDefinitionVersion as default without it having a versionId.")
    }

    val currentDefaultVersionOpt = actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(actorDefinitionId)
    currentDefaultVersionOpt.ifPresent { currentDefaultVersion ->
      val breakingChangesForUpgrade =
        getBreakingChangesForUpgrade(
          currentDefaultVersion.dockerImageTag,
          newDefaultVersion.dockerImageTag,
          breakingChangesForDefinition,
        )

      // Determine which actors should NOT be upgraded, and pin those back
      processBreakingChangesForUpgrade(currentDefaultVersion, breakingChangesForUpgrade)
    }

    actorDefinitionService.updateActorDefinitionDefaultVersionId(actorDefinitionId, newDefaultVersion.versionId)

    // For breaking changes that have been rolled back, clear old pins that may have been created
    processBreakingChangePinRollbacks(actorDefinitionId, newDefaultVersion, breakingChangesForDefinition)
  }

  @VisibleForTesting
  fun getConfigScopeMaps(actorDefinitionId: UUID): Collection<ConfigScopeMapWithId> {
    val actorScopes = actorDefinitionService.getActorIdsForDefinition(actorDefinitionId)
    return actorScopes.map {
      ConfigScopeMapWithId(
        it.actorId,
        mapOf(
          ConfigScopeType.ACTOR to it.actorId,
          ConfigScopeType.WORKSPACE to it.workspaceId,
          ConfigScopeType.ORGANIZATION to it.organizationId,
        ),
      )
    }
  }

  private fun getActorIdsToPinForBreakingChange(
    actorDefinitionId: UUID,
    breakingChange: ActorDefinitionBreakingChange,
    configScopeMaps: Collection<ConfigScopeMapWithId>,
  ): Set<UUID> {
    // upgrade candidates: any actor that doesn't have a pin on it
    // this must happen in order, for each BC, so when processing multiple breaking changes at once we
    // determine affected actors correctly
    val upgradeCandidates = getUpgradeCandidates(actorDefinitionId, configScopeMaps)

    // actors to pin: any actor from candidates (no pins) that is impacted by a breaking change
    return getActorsAffectedByBreakingChange(upgradeCandidates, breakingChange)
  }

  @VisibleForTesting
  internal fun processBreakingChangesForUpgrade(
    currentDefaultVersion: ActorDefinitionVersion,
    breakingChangesForUpgrade: List<ActorDefinitionBreakingChange>,
  ) {
    if (breakingChangesForUpgrade.isEmpty()) return

    val actorDefinitionId = currentDefaultVersion.actorDefinitionId
    val configScopeMaps = getConfigScopeMaps(actorDefinitionId)
    for (breakingChange in breakingChangesForUpgrade) {
      val actorIdsToPin = getActorIdsToPinForBreakingChange(actorDefinitionId, breakingChange, configScopeMaps)
      if (actorIdsToPin.isNotEmpty()) {
        // create the pins
        createBreakingChangePinsForActors(actorIdsToPin, currentDefaultVersion, breakingChange)
      }
    }
  }

  /**
   * For breaking changes that have been rolled back, clear old pins that may have been created.
   * Removing the pins will cause the actors to use the new default version.
   */
  @VisibleForTesting
  internal fun processBreakingChangePinRollbacks(
    actorDefinitionId: UUID,
    newDefaultVersion: ActorDefinitionVersion,
    breakingChangesForDef: List<ActorDefinitionBreakingChange>,
  ) {
    val rolledBackBreakingChanges =
      getBreakingChangesAfterVersion(
        newDefaultVersion.dockerImageTag,
        breakingChangesForDef,
      )

    if (rolledBackBreakingChanges.isEmpty()) return

    val scopedConfigsToRemove =
      scopedConfigurationService.listScopedConfigurationsWithOrigins(
        ConnectorVersionKey.key,
        ConfigResourceType.ACTOR_DEFINITION,
        actorDefinitionId,
        ConfigOriginType.BREAKING_CHANGE,
        rolledBackBreakingChanges.map { it.version.serialize() },
      )

    if (scopedConfigsToRemove.isNotEmpty()) {
      scopedConfigurationService.deleteScopedConfigurations(scopedConfigsToRemove.map { it.id })
    }
  }

  @VisibleForTesting
  fun getUpgradeCandidates(
    actorDefinitionId: UUID,
    configScopeMaps: Collection<ConfigScopeMapWithId>,
  ): Set<UUID> {
    val scopedConfigs =
      scopedConfigurationService.getScopedConfigurations(
        ConnectorVersionKey,
        ConfigResourceType.ACTOR_DEFINITION,
        actorDefinitionId,
        configScopeMaps.toList(),
      )

    // upgrade candidates are all those actorIds that don't have a version config
    return configScopeMaps.stream()
      .filter { !scopedConfigs.containsKey(it.id) }
      .map { it.id }
      .collect(Collectors.toSet())
  }

  @VisibleForTesting
  internal fun createBreakingChangePinsForActors(
    actorIds: Set<UUID>,
    currentVersion: ActorDefinitionVersion,
    breakingChange: ActorDefinitionBreakingChange,
  ) {
    val scopedConfigurationsToCreate =
      actorIds.map { actorId ->
        ScopedConfiguration()
          .withId(UUID.randomUUID())
          .withKey(ConnectorVersionKey.key)
          .withValue(currentVersion.versionId.toString())
          .withResourceType(ConfigResourceType.ACTOR_DEFINITION)
          .withResourceId(currentVersion.actorDefinitionId)
          .withScopeType(ConfigScopeType.ACTOR)
          .withScopeId(actorId)
          .withOriginType(ConfigOriginType.BREAKING_CHANGE)
          .withOrigin(breakingChange.version.serialize())
      }.toList()
    scopedConfigurationService.insertScopedConfigurations(scopedConfigurationsToCreate)
  }

  @VisibleForTesting
  fun createReleaseCandidatePinsForActors(
    actorIds: Set<UUID>,
    actorDefinitionId: UUID,
    releaseCandidateVersionId: UUID,
    rolloutId: UUID,
  ) {
    val configScopeMaps = getConfigScopeMaps(actorDefinitionId)
    val allEligibleActorIds =
      getUpgradeCandidates(
        actorDefinitionId,
        configScopeMaps,
      )

    val ineligibleActorIds = actorIds.toSet() - allEligibleActorIds
    if (ineligibleActorIds.isNotEmpty()) {
      throw InvalidRequestException("Rollout update failed; the following actors do not exist or are already pinned: $ineligibleActorIds")
    }

    val scopedConfigurationsToCreate =
      actorIds.map { actorId ->
        ScopedConfiguration()
          .withId(UUID.randomUUID())
          .withKey(ConnectorVersionKey.key)
          .withValue(releaseCandidateVersionId.toString())
          .withResourceType(ConfigResourceType.ACTOR_DEFINITION)
          .withResourceId(actorDefinitionId)
          .withScopeType(ConfigScopeType.ACTOR)
          .withScopeId(actorId)
          .withOriginType(ConfigOriginType.CONNECTOR_ROLLOUT)
          .withOrigin(rolloutId.toString())
      }.toList()
    scopedConfigurationService.insertScopedConfigurations(scopedConfigurationsToCreate)
  }

  fun migrateReleaseCandidatePins(
    actorDefinitionId: UUID,
    origins: List<String>,
    newOrigin: String,
    newReleaseCandidateVersionId: UUID,
  ) {
    scopedConfigurationService.updateScopedConfigurationsOriginAndValuesForOriginInList(
      ConnectorVersionKey.key,
      ConfigResourceType.ACTOR_DEFINITION,
      actorDefinitionId,
      ConfigOriginType.CONNECTOR_ROLLOUT,
      origins,
      newOrigin,
      newReleaseCandidateVersionId.toString(),
    )
  }

  @VisibleForTesting
  fun removeReleaseCandidatePinsForVersion(
    actorDefinitionId: UUID,
    releaseCandidateVersionId: UUID,
  ) {
    val scopedConfigsToRemove =
      scopedConfigurationService.listScopedConfigurationsWithValues(
        ConnectorVersionKey.key,
        ConfigResourceType.ACTOR_DEFINITION,
        actorDefinitionId,
        ConfigScopeType.ACTOR,
        ConfigOriginType.CONNECTOR_ROLLOUT,
        listOf(releaseCandidateVersionId.toString()),
      )

    if (scopedConfigsToRemove.isNotEmpty()) {
      scopedConfigurationService.deleteScopedConfigurations(scopedConfigsToRemove.map { it.id })
    }
  }

  @VisibleForTesting
  fun getActorsAffectedByBreakingChange(
    actorIds: Set<UUID>,
    breakingChange: ActorDefinitionBreakingChange,
  ): Set<UUID> {
    if (!featureFlagClient.boolVariation(UseBreakingChangeScopes, Workspace(ANONYMOUS))) {
      return actorIds
    }

    val scopedImpact = breakingChange.scopedImpact
    if (breakingChange.scopedImpact == null || breakingChange.scopedImpact.isEmpty()) {
      return actorIds
    }

    val actorsImpactedByBreakingChange: MutableSet<UUID> = HashSet()
    for (impactScope in scopedImpact) {
      if (impactScope.scopeType == BreakingChangeScope.ScopeType.STREAM) {
        val streamBreakingChangeScope = BreakingChangeScopeFactory.createStreamBreakingChangeScope(impactScope)
        actorsImpactedByBreakingChange.addAll(getActorsInStreamBreakingChangeScope(actorIds, streamBreakingChangeScope))
      } else {
        throw RuntimeException("Unsupported breaking change scope type: " + impactScope.scopeType)
      }
    }

    return actorsImpactedByBreakingChange
  }

  private fun getActorsInStreamBreakingChangeScope(
    actorIdsToFilter: Set<UUID>,
    streamBreakingChangeScope: StreamBreakingChangeScope,
  ): Set<UUID> {
    return actorIdsToFilter
      .stream()
      .filter { actorId: UUID ->
        getActorSyncsAnyListedStream(
          actorId,
          streamBreakingChangeScope.impactedScopes,
        )
      }
      .collect(Collectors.toSet())
  }

  private fun getActorSyncsAnyListedStream(
    actorId: UUID,
    streamNames: List<String>,
  ): Boolean {
    try {
      return connectionService.actorSyncsAnyListedStream(actorId, streamNames)
    } catch (e: IOException) {
      throw java.lang.RuntimeException(e)
    }
  }

  /**
   * Given a current version and a version to upgrade to, and a list of breaking changes, determine
   * which breaking changes, if any, apply to upgrading from the current version to the version to
   * upgrade to.
   *
   * @param currentDockerImageTag version to upgrade from
   * @param dockerImageTagForUpgrade version to upgrade to
   * @param breakingChangesForDef a list of breaking changes to check
   * @return list of applicable breaking changes
   */
  @VisibleForTesting
  internal fun getBreakingChangesForUpgrade(
    currentDockerImageTag: String,
    dockerImageTagForUpgrade: String,
    breakingChangesForDef: List<ActorDefinitionBreakingChange>,
  ): List<ActorDefinitionBreakingChange> {
    if (breakingChangesForDef.isEmpty()) {
      // If there aren't breaking changes, early exit in order to avoid trying to parse versions.
      // This is helpful for custom connectors or local dev images for connectors that don't have
      // breaking changes.
      return listOf()
    }

    val currentVersion = Version(currentDockerImageTag)
    val versionToUpgradeTo = Version(dockerImageTagForUpgrade)

    if (versionToUpgradeTo.lessThanOrEqualTo(currentVersion)) {
      // When downgrading, we don't take into account breaking changes.
      return listOf()
    }

    return breakingChangesForDef.stream().filter { breakingChange ->
      (
        currentVersion.lessThan(breakingChange.version) &&
          versionToUpgradeTo.greaterThanOrEqualTo(breakingChange.version)
      )
    }.sorted { bc1, bc2 -> bc1.version.versionCompareTo(bc2.version) }.toList()
  }

  /**
   * Given a new image tag, and a list of breaking changes, determine which breaking changes, if any,
   * are after the new version (i.e. are not applicable to the new version).
   */
  @VisibleForTesting
  internal fun getBreakingChangesAfterVersion(
    newImageTag: String,
    breakingChangesForDef: List<ActorDefinitionBreakingChange>,
  ): List<ActorDefinitionBreakingChange> {
    if (breakingChangesForDef.isEmpty()) {
      return listOf()
    }

    val newVersion = Version(newImageTag)
    return breakingChangesForDef.filter { it.version.greaterThan(newVersion) }.toList()
  }
}
