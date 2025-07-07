/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers

import io.airbyte.commons.version.AirbyteProtocolVersion
import io.airbyte.commons.version.Version
import io.airbyte.config.ActorDefinitionBreakingChange
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.BreakingChangeScope
import io.airbyte.config.ConnectorEnumRolloutState
import io.airbyte.config.ConnectorRegistryDestinationDefinition
import io.airbyte.config.ConnectorRegistrySourceDefinition
import io.airbyte.config.ConnectorRollout
import io.airbyte.config.RolloutConfiguration
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.SupportLevel
import io.airbyte.config.VersionBreakingChange
import io.airbyte.protocol.models.v0.ConnectorSpecification
import java.time.OffsetDateTime
import java.util.UUID
import java.util.stream.Collectors

/**
 * Utility class for converting between the connector registry and platform types.
 */
object ConnectorRegistryConverters {
  val DEFAULT_ROLLOUT_CONFIGURATION: RolloutConfiguration =
    RolloutConfiguration().withInitialPercentage(0L).withMaxPercentage(0L).withAdvanceDelayMinutes(0L)

  /**
   * Convert the connector registry source type to the platform source definition type.
   */
  @JvmStatic
  fun toStandardSourceDefinition(def: ConnectorRegistrySourceDefinition): StandardSourceDefinition {
    val metrics = def.generated?.metrics

    return StandardSourceDefinition()
      .withSourceDefinitionId(def.sourceDefinitionId)
      .withName(def.name)
      .withIcon(def.icon)
      .withIconUrl(def.iconUrl)
      .withSourceType(toStandardSourceType(def.sourceType))
      .withTombstone(def.tombstone)
      .withPublic(def.public)
      .withCustom(def.custom)
      .withEnterprise(if (def.abInternal != null) def.abInternal.isEnterprise else false)
      .withMetrics(metrics)
      .withResourceRequirements(def.resourceRequirements)
      .withMaxSecondsBetweenMessages(def.maxSecondsBetweenMessages)
  }

  /**
   * Convert the connector registry destination type to the platform destination definition type.
   */
  @JvmStatic
  fun toStandardDestinationDefinition(def: ConnectorRegistryDestinationDefinition): StandardDestinationDefinition {
    val metrics = def.generated?.metrics

    return StandardDestinationDefinition()
      .withDestinationDefinitionId(def.destinationDefinitionId)
      .withName(def.name)
      .withIcon(def.icon)
      .withIconUrl(def.iconUrl)
      .withTombstone(def.tombstone)
      .withPublic(def.public)
      .withCustom(def.custom)
      .withEnterprise(if (def.abInternal != null) def.abInternal.isEnterprise else false)
      .withMetrics(metrics)
      .withResourceRequirements(def.resourceRequirements)
  }

  /**
   * Convert the version-related fields of the ConnectorRegistrySourceDefinition into an
   * ActorDefinitionVersion.
   */
  @JvmStatic
  fun toActorDefinitionVersion(def: ConnectorRegistrySourceDefinition): ActorDefinitionVersion {
    val lastModified = def.generated?.sourceFileInfo?.metadataLastModified
    val cdkVersion = def.packageInfo?.cdkVersion

    validateDockerImageTag(def.dockerImageTag)
    return ActorDefinitionVersion()
      .withActorDefinitionId(def.sourceDefinitionId)
      .withDockerRepository(def.dockerRepository)
      .withDockerImageTag(def.dockerImageTag)
      .withSpec(def.spec)
      .withAllowedHosts(def.allowedHosts)
      .withDocumentationUrl(def.documentationUrl)
      .withProtocolVersion(getProtocolVersion(def.spec))
      .withReleaseDate(def.releaseDate)
      .withSupportLevel(def.supportLevel ?: SupportLevel.NONE)
      .withInternalSupportLevel(def.abInternal?.sl ?: 100L)
      .withReleaseStage(def.releaseStage)
      .withLastPublished(lastModified)
      .withCdkVersion(cdkVersion)
      .withSuggestedStreams(def.suggestedStreams)
      .withLanguage(def.language)
      .withSupportsFileTransfer(def.supportsFileTransfer)
      .withConnectorIPCOptions(def.connectorIPCOptions)
  }

  /**
   * Convert the version-related fields of the ConnectorRegistrySourceDefinition into an
   * ActorDefinitionVersion.
   */
  @JvmStatic
  fun toActorDefinitionVersion(def: ConnectorRegistryDestinationDefinition): ActorDefinitionVersion {
    val lastModified = def.generated?.sourceFileInfo?.metadataLastModified
    val cdkVersion = def.packageInfo?.cdkVersion

    validateDockerImageTag(def.dockerImageTag)
    return ActorDefinitionVersion()
      .withActorDefinitionId(def.destinationDefinitionId)
      .withDockerRepository(def.dockerRepository)
      .withDockerImageTag(def.dockerImageTag)
      .withSpec(def.spec)
      .withAllowedHosts(def.allowedHosts)
      .withDocumentationUrl(def.documentationUrl)
      .withProtocolVersion(getProtocolVersion(def.spec))
      .withReleaseDate(def.releaseDate)
      .withReleaseStage(def.releaseStage)
      .withSupportLevel(def.supportLevel ?: SupportLevel.NONE)
      .withInternalSupportLevel(def.abInternal?.sl ?: 100L)
      .withLastPublished(lastModified)
      .withCdkVersion(cdkVersion)
      .withSupportsRefreshes(def.supportsRefreshes != null && def.supportsRefreshes)
      .withLanguage(def.language)
      .withSupportsFileTransfer(def.supportsFileTransfer)
      .withSupportsDataActivation(def.supportsDataActivation)
      .withConnectorIPCOptions(def.connectorIPCOptions)
  }

  /**
   * Convert the breaking-change-related fields of the ConnectorRegistrySourceDefinition into a list
   * of ActorDefinitionBreakingChanges.
   */
  @JvmStatic
  fun toActorDefinitionBreakingChanges(def: ConnectorRegistrySourceDefinition?): List<ActorDefinitionBreakingChange> {
    if (def?.releases?.breakingChanges == null) {
      return emptyList()
    }

    val breakingChangeMap = def.releases.breakingChanges.additionalProperties
    return toActorDefinitionBreakingChanges(breakingChangeMap, def.sourceDefinitionId)
  }

  /**
   * Convert the breaking-change-related fields of the ConnectorRegistryDestinationDefinition into a
   * list of ActorDefinitionBreakingChanges.
   */
  @JvmStatic
  fun toActorDefinitionBreakingChanges(def: ConnectorRegistryDestinationDefinition?): List<ActorDefinitionBreakingChange> {
    if (def?.releases?.breakingChanges == null) {
      return emptyList()
    }

    val breakingChangeMap = def.releases.breakingChanges.additionalProperties
    return toActorDefinitionBreakingChanges(breakingChangeMap, def.destinationDefinitionId)
  }

  private fun toActorDefinitionBreakingChanges(
    breakingChangeMap: Map<String, VersionBreakingChange>,
    actorDefinitionID: UUID,
  ): List<ActorDefinitionBreakingChange> =
    breakingChangeMap.entries
      .stream()
      .map { entry: Map.Entry<String, VersionBreakingChange> ->
        ActorDefinitionBreakingChange()
          .withActorDefinitionId(actorDefinitionID)
          .withVersion(Version(entry.key))
          .withMigrationDocumentationUrl(entry.value.migrationDocumentationUrl)
          .withUpgradeDeadline(entry.value.upgradeDeadline)
          .withDeadlineAction(entry.value.deadlineAction)
          .withMessage(entry.value.message)
          .withScopedImpact(getValidatedScopedImpact(entry.value.scopedImpact))
      }.collect(Collectors.toList())

  private fun toStandardSourceType(sourceType: ConnectorRegistrySourceDefinition.SourceType?): StandardSourceDefinition.SourceType? {
    if (sourceType == null) {
      return null
    }

    return when (sourceType) {
      ConnectorRegistrySourceDefinition.SourceType.API -> {
        StandardSourceDefinition.SourceType.API
      }

      ConnectorRegistrySourceDefinition.SourceType.FILE -> {
        StandardSourceDefinition.SourceType.FILE
      }

      ConnectorRegistrySourceDefinition.SourceType.DATABASE -> {
        StandardSourceDefinition.SourceType.DATABASE
      }

      ConnectorRegistrySourceDefinition.SourceType.CUSTOM -> {
        StandardSourceDefinition.SourceType.CUSTOM
      }

      else -> throw IllegalArgumentException("Unknown source type: $sourceType")
    }
  }

  @JvmStatic
  fun toConnectorRollout(
    rcDef: ConnectorRegistrySourceDefinition,
    rcAdv: ActorDefinitionVersion,
    initialAdv: ActorDefinitionVersion,
  ): ConnectorRollout {
    assert(rcDef.sourceDefinitionId == rcAdv.actorDefinitionId)
    assert(rcDef.dockerRepository == rcAdv.dockerRepository)
    assert(rcDef.dockerImageTag == rcAdv.dockerImageTag)
    if (rcDef.releases != null) {
      val hasBreakingChange =
        rcDef.releases.breakingChanges != null &&
          rcDef.releases.breakingChanges.additionalProperties
            .containsKey(rcDef.dockerImageTag)
      val rolloutConfiguration = rcDef.releases.rolloutConfiguration
      return toConnectorRollout(rolloutConfiguration, rcAdv, initialAdv, hasBreakingChange)
    }
    return toConnectorRollout(null, rcAdv, initialAdv, false)
  }

  fun toConnectorRollout(
    rcDef: ConnectorRegistryDestinationDefinition,
    rcAdv: ActorDefinitionVersion,
    initialAdv: ActorDefinitionVersion,
  ): ConnectorRollout {
    assert(rcDef.destinationDefinitionId == rcAdv.actorDefinitionId)
    assert(rcDef.dockerRepository == rcAdv.dockerRepository)
    assert(rcDef.dockerImageTag == rcAdv.dockerImageTag)
    if (rcDef.releases != null) {
      val hasBreakingChange =
        rcDef.releases.breakingChanges != null &&
          rcDef.releases.breakingChanges.additionalProperties
            .containsKey(rcDef.dockerImageTag)
      val rolloutConfiguration = rcDef.releases.rolloutConfiguration
      return toConnectorRollout(rolloutConfiguration, rcAdv, initialAdv, hasBreakingChange)
    }
    return toConnectorRollout(null, rcAdv, initialAdv, false)
  }

  private fun toConnectorRollout(
    rolloutConfiguration: RolloutConfiguration?,
    rcAdv: ActorDefinitionVersion,
    initialAdv: ActorDefinitionVersion,
    hasBreakingChange: Boolean,
  ): ConnectorRollout {
    val connectorRollout =
      ConnectorRollout(
        id = UUID.randomUUID(),
        workflowRunId = null,
        actorDefinitionId = rcAdv.actorDefinitionId,
        releaseCandidateVersionId = rcAdv.versionId,
        initialVersionId = initialAdv.versionId,
        state = ConnectorEnumRolloutState.INITIALIZED,
        initialRolloutPct =
          (rolloutConfiguration?.initialPercentage ?: DEFAULT_ROLLOUT_CONFIGURATION.initialPercentage)
            .toInt(),
        currentTargetRolloutPct = null,
        finalTargetRolloutPct = (rolloutConfiguration?.maxPercentage ?: DEFAULT_ROLLOUT_CONFIGURATION.maxPercentage).toInt(),
        hasBreakingChanges = hasBreakingChange,
        rolloutStrategy = null,
        maxStepWaitTimeMins =
          (rolloutConfiguration?.advanceDelayMinutes ?: DEFAULT_ROLLOUT_CONFIGURATION.advanceDelayMinutes)
            .toInt(),
        updatedBy = null,
        createdAt = OffsetDateTime.now().toEpochSecond(),
        updatedAt = OffsetDateTime.now().toEpochSecond(),
        completedAt = null,
        expiresAt = null,
        errorMsg = null,
        failedReason = null,
        pausedReason = null,
        filters = null,
        tag = null,
      )

    return connectorRollout
  }

  @JvmStatic
  fun toRcSourceDefinitions(def: ConnectorRegistrySourceDefinition?): List<ConnectorRegistrySourceDefinition> {
    if (def?.releases?.releaseCandidates == null) {
      return emptyList()
    }

    return def
      .releases
      .releaseCandidates
      .additionalProperties.values
      .filterNotNull()
      .toList()
  }

  @JvmStatic
  fun toRcDestinationDefinitions(def: ConnectorRegistryDestinationDefinition?): List<ConnectorRegistryDestinationDefinition> {
    if (def?.releases?.releaseCandidates == null) {
      return emptyList()
    }

    return def
      .releases
      .releaseCandidates
      .additionalProperties.values
      .filterNotNull()
      .toList()
  }

  private fun getProtocolVersion(spec: ConnectorSpecification?): String = AirbyteProtocolVersion.getWithDefault(spec?.protocolVersion).serialize()

  private fun validateDockerImageTag(dockerImageTag: String) {
    try {
      Version(dockerImageTag)
    } catch (e: IllegalArgumentException) {
      throw IllegalArgumentException("Invalid Semver version for docker image tag: $dockerImageTag", e)
    }
  }

  /**
   * jsonschema2Pojo does not support oneOf and const Therefore, the type checking for
   * BreakingChangeScope cannot take more specific subtypes. However, we want to validate that each
   * scope can be correctly resolved to an internal type that we'll use for processing later (e.g.
   * StreamBreakingChangeScope), So we validate that here at runtime instead.
   */
  private fun getValidatedScopedImpact(scopedImpact: List<BreakingChangeScope>): List<BreakingChangeScope> {
    scopedImpact.forEach { BreakingChangeScopeFactory.validateBreakingChangeScope(it) }
    return scopedImpact
  }
}
