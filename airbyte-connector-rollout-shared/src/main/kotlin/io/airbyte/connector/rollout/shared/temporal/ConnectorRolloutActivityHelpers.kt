/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.shared

import io.airbyte.api.client.model.generated.ConnectorRolloutActorSelectionInfo
import io.airbyte.api.client.model.generated.ConnectorRolloutActorSyncInfo
import io.airbyte.api.client.model.generated.ConnectorRolloutRead
import io.airbyte.api.client.model.generated.ConnectorRolloutState
import io.airbyte.api.client.model.generated.ConnectorRolloutStrategy
import io.airbyte.config.ConnectorEnumRolloutState
import io.airbyte.config.ConnectorEnumRolloutStrategy
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutOutput
import java.util.UUID

typealias ModelConnectorRolloutActorSelectionInfo = io.airbyte.api.model.generated.ConnectorRolloutActorSelectionInfo
typealias ModelActorSyncInfo = io.airbyte.api.model.generated.ConnectorRolloutActorSyncInfo

object ConnectorRolloutActivityHelpers {
  fun mapToConnectorRollout(rolloutRead: ConnectorRolloutRead): ConnectorRolloutOutput =
    ConnectorRolloutOutput(
      id = rolloutRead.id,
      workflowRunId = rolloutRead.workflowRunId,
      actorDefinitionId = rolloutRead.actorDefinitionId,
      releaseCandidateVersionId = rolloutRead.releaseCandidateVersionId,
      initialVersionId = rolloutRead.initialVersionId,
      state = mapState(rolloutRead.state),
      initialRolloutPct = rolloutRead.initialRolloutPct,
      currentTargetRolloutPct = rolloutRead.currentTargetRolloutPct,
      finalTargetRolloutPct = rolloutRead.finalTargetRolloutPct,
      hasBreakingChanges = rolloutRead.hasBreakingChanges,
      rolloutStrategy = mapRolloutStrategy(rolloutRead.rolloutStrategy),
      maxStepWaitTimeMins = rolloutRead.maxStepWaitTimeMins,
      updatedBy = rolloutRead.updatedBy,
      createdAt = rolloutRead.createdAt,
      updatedAt = rolloutRead.updatedAt,
      completedAt = rolloutRead.completedAt,
      expiresAt = rolloutRead.expiresAt,
      errorMsg = rolloutRead.errorMsg,
      failedReason = rolloutRead.failedReason,
      pausedReason = rolloutRead.pausedReason,
      actorSelectionInfo = mapActorSelectionInfo(rolloutRead.actorSelectionInfo),
      actorSyncs = mapActorSyncs(rolloutRead.actorSyncs),
    )

  private fun mapActorSelectionInfo(actorSelectionInfo: ConnectorRolloutActorSelectionInfo?): ModelConnectorRolloutActorSelectionInfo? =
    if (actorSelectionInfo == null) {
      null
    } else {
      ModelConnectorRolloutActorSelectionInfo()
        .numActors(actorSelectionInfo.numActors)
        .numPinnedToConnectorRollout(actorSelectionInfo.numPinnedToConnectorRollout)
        .numActorsEligibleOrAlreadyPinned((actorSelectionInfo.numActorsEligibleOrAlreadyPinned))
    }

  private fun mapActorSyncs(actorSyncs: Map<String, ConnectorRolloutActorSyncInfo>?): Map<UUID, ModelActorSyncInfo>? =
    actorSyncs
      ?.map {
        UUID.fromString(it.key) to
          ModelActorSyncInfo()
            .actorId(it.value.actorId)
            .numFailed(it.value.numFailed)
            .numSucceeded(it.value.numSucceeded)
            .numConnections(it.value.numConnections)
      }?.toMap()

  private fun mapState(state: ConnectorRolloutState): ConnectorEnumRolloutState = state.let { ConnectorEnumRolloutState.valueOf(it.name) }

  private fun mapRolloutStrategy(strategy: ConnectorRolloutStrategy?): ConnectorEnumRolloutStrategy? =
    strategy?.let {
      ConnectorEnumRolloutStrategy.valueOf(it.name)
    }
}
