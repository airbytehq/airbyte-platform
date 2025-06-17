/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import com.google.common.annotations.VisibleForTesting
import io.airbyte.api.model.generated.ConnectorRolloutActorSelectionInfo
import io.airbyte.api.model.generated.ConnectorRolloutActorSyncInfo
import io.airbyte.api.model.generated.ConnectorRolloutRead
import io.airbyte.api.model.generated.ConnectorRolloutState
import io.airbyte.api.model.generated.ConnectorRolloutStrategy
import io.airbyte.config.ConnectorEnumRolloutStrategy
import io.airbyte.config.ConnectorRollout
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.connector.rollout.shared.RolloutActorFinder
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectorRolloutService
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.cache.annotation.Cacheable
import jakarta.inject.Inject
import jakarta.inject.Singleton
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID

val logger = KotlinLogging.logger { }

/**
 * OperationsHandler. Javadocs suppressed because api docs should be used as source of truth.
 */
@Singleton
open class ConnectorRolloutHelper
  @Inject
  constructor(
    private val connectorRolloutService: ConnectorRolloutService,
    private val actorDefinitionService: ActorDefinitionService,
    private val userPersistence: UserPersistence,
    private val rolloutActorFinder: RolloutActorFinder,
  ) {
    @VisibleForTesting
    open fun buildConnectorRolloutRead(
      connectorRollout: ConnectorRollout,
      withActorSyncAndSelectionInfo: Boolean,
    ): ConnectorRolloutRead {
      val rolloutStrategy = connectorRollout.rolloutStrategy?.let { ConnectorRolloutStrategy.fromValue(it.toString()) }

      val actorDefinitionVersion = actorDefinitionService.getActorDefinitionVersion(connectorRollout.releaseCandidateVersionId)
      var rollout =
        ConnectorRolloutRead()
          .id(connectorRollout.id)
          .dockerRepository(actorDefinitionVersion.dockerRepository)
          .dockerImageTag(actorDefinitionVersion.dockerImageTag)
          .workflowRunId(connectorRollout.workflowRunId)
          .actorDefinitionId(connectorRollout.actorDefinitionId)
          .releaseCandidateVersionId(connectorRollout.releaseCandidateVersionId)
          .initialVersionId(connectorRollout.initialVersionId)
          .state(ConnectorRolloutState.fromString(connectorRollout.state.toString()))
          .initialRolloutPct(connectorRollout.initialRolloutPct)
          .currentTargetRolloutPct(connectorRollout.currentTargetRolloutPct)
          .finalTargetRolloutPct(connectorRollout.finalTargetRolloutPct)
          .hasBreakingChanges(connectorRollout.hasBreakingChanges)
          .rolloutStrategy(rolloutStrategy)
          .maxStepWaitTimeMins(connectorRollout.maxStepWaitTimeMins)
          .updatedAt(unixTimestampToOffsetDateTime(connectorRollout.updatedAt))
          .createdAt(unixTimestampToOffsetDateTime(connectorRollout.createdAt))
          .expiresAt(connectorRollout.expiresAt?.let { unixTimestampToOffsetDateTime(it) })
          .errorMsg(connectorRollout.errorMsg)
          .failedReason(connectorRollout.failedReason)
          .pausedReason(connectorRollout.pausedReason)
          .updatedBy(
            connectorRollout.rolloutStrategy?.let { strategy ->
              connectorRollout.updatedBy?.let { updatedBy ->
                getUpdatedBy(strategy, updatedBy)
              }
            },
          ).completedAt(connectorRollout.completedAt?.let { unixTimestampToOffsetDateTime(it) })
          .expiresAt(connectorRollout.expiresAt?.let { unixTimestampToOffsetDateTime(it) })
          .tag(connectorRollout.tag)

      if (withActorSyncAndSelectionInfo) {
        val pinnedActorInfo = getActorSelectionInfoForPinnedActors(connectorRollout.id)
        val actorSyncInfo = getActorSyncInfo(connectorRollout.id).mapKeys { (uuidKey, _) -> uuidKey.toString() }

        logger.info {
          "buildConnectorRolloutRead withActorSyncAndSelectionInfo " +
            "rolloutId=${connectorRollout.id} " +
            "numPinnedToConnectorRollout=${pinnedActorInfo.numPinnedToConnectorRollout} " +
            "actorSyncInfo=${actorSyncInfo.entries.associate { it.key to it.value.toString() }}"
        }

        rollout =
          rollout
            .actorSelectionInfo(pinnedActorInfo)
            .actorSyncs(actorSyncInfo)
      }
      return rollout
    }

    private fun unixTimestampToOffsetDateTime(unixTimestamp: Long): OffsetDateTime = Instant.ofEpochSecond(unixTimestamp).atOffset(ZoneOffset.UTC)

    @Cacheable("rollout-updated-by")
    open fun getUpdatedBy(
      rolloutStrategy: ConnectorEnumRolloutStrategy,
      updatedById: UUID,
    ): String =
      when (rolloutStrategy) {
        ConnectorEnumRolloutStrategy.MANUAL -> userPersistence.getUser(updatedById).get().email
        else -> ""
      }

    fun getActorSelectionInfoForPinnedActors(id: UUID): ConnectorRolloutActorSelectionInfo {
      val rollout = connectorRolloutService.getConnectorRollout(id)
      logger.info { "getActorSelectionInfoForPinnedActors: rollout=$rollout" }
      val actorSelectionInfo = rolloutActorFinder.getActorSelectionInfo(rollout, null, rollout.filters)
      logger.info { "getActorSelectionInfoForPinnedActors: actorSelectionInfo=$actorSelectionInfo" }

      return ConnectorRolloutActorSelectionInfo()
        .numActors(actorSelectionInfo.nActors)
        .numPinnedToConnectorRollout(actorSelectionInfo.nPreviouslyPinned)
        .numActorsEligibleOrAlreadyPinned(actorSelectionInfo.nActorsEligibleOrAlreadyPinned)
    }

    fun getActorSyncInfo(id: UUID): Map<UUID, ConnectorRolloutActorSyncInfo> {
      val rollout = connectorRolloutService.getConnectorRollout(id)
      val actorSyncInfoMap = rolloutActorFinder.getSyncInfoForPinnedActors(rollout)
      return actorSyncInfoMap.mapValues { (id, syncInfo) ->
        ConnectorRolloutActorSyncInfo()
          .actorId(id)
          .numConnections(syncInfo.nConnections)
          .numSucceeded(syncInfo.nSucceeded)
          .numFailed(syncInfo.nFailed)
      }
    }
  }
