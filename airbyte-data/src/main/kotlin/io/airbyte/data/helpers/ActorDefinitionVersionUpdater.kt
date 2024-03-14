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
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.helpers.BreakingChangeScopeFactory
import io.airbyte.config.helpers.StreamBreakingChangeScope
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.shared.ConnectorVersionKey
import io.airbyte.featureflag.ANONYMOUS
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.UseBreakingChangeScopes
import io.airbyte.featureflag.Workspace
import jakarta.inject.Singleton
import java.io.IOException
import java.util.UUID
import java.util.stream.Collectors

@Singleton
class ActorDefinitionVersionUpdater(
  private val featureFlagClient: FeatureFlagClient,
  private val connectionService: ConnectionService,
  private val actorDefinitionService: ActorDefinitionService,
  private val scopedConfigurationService: ScopedConfigurationService,
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
      sourceDefinition.defaultVersionId,
      ActorType.SOURCE,
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
      destinationDefinition.defaultVersionId,
      ActorType.DESTINATION,
    )
  }

  @VisibleForTesting
  fun upgradeActorVersion(
    actorId: UUID,
    actorDefinitionId: UUID,
    newVersionId: UUID,
    actorType: ActorType,
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
    }

    actorDefinitionService.setActorDefaultVersion(actorId, newVersionId)
  }

  @VisibleForTesting
  fun updateDefaultVersion(
    actorDefinitionId: UUID,
    newDefaultVersion: ActorDefinitionVersion,
    breakingChangesForDefinition: List<ActorDefinitionBreakingChange>,
  ) {
    if (newDefaultVersion.versionId == null) {
      throw RuntimeException("Can't set an actorDefinitionVersion as default without it having a versionId.")
    }

    val currentDefaultVersionOpt = actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(actorDefinitionId)
    currentDefaultVersionOpt.ifPresent { currentDefaultVersion ->
      val actorsToUpgrade = getActorsToUpgrade(currentDefaultVersion, newDefaultVersion, breakingChangesForDefinition)
      actorDefinitionService.setActorDefaultVersions(actorsToUpgrade.stream().toList(), newDefaultVersion.versionId)
    }

    actorDefinitionService.updateActorDefinitionDefaultVersionId(actorDefinitionId, newDefaultVersion.versionId)
  }

  @VisibleForTesting
  fun getActorsToUpgrade(
    currentDefaultVersion: ActorDefinitionVersion,
    newVersion: ActorDefinitionVersion,
    breakingChangesForDefinition: List<ActorDefinitionBreakingChange>,
  ): Set<UUID> {
    val breakingChangesForUpgrade: List<ActorDefinitionBreakingChange> =
      getBreakingChangesForUpgrade(
        currentDefaultVersion.dockerImageTag,
        newVersion.dockerImageTag,
        breakingChangesForDefinition,
      )

    val upgradeCandidates = actorDefinitionService.getActorsWithDefaultVersionId(currentDefaultVersion.versionId).toMutableSet()

    for (breakingChange in breakingChangesForUpgrade) {
      val actorsImpactedByBreakingChange = getActorsAffectedByBreakingChange(upgradeCandidates, breakingChange)
      upgradeCandidates.removeAll(actorsImpactedByBreakingChange)
    }

    return upgradeCandidates
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
  fun getBreakingChangesForUpgrade(
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

    return breakingChangesForDef.stream().filter { breakingChange: ActorDefinitionBreakingChange ->
      (
        currentVersion.lessThan(breakingChange.version) &&
          versionToUpgradeTo.greaterThanOrEqualTo(breakingChange.version)
      )
    }.collect(Collectors.toList())
  }
}
