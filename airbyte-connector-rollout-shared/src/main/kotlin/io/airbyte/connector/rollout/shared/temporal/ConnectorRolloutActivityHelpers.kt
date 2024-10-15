/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.shared

import io.airbyte.api.client.model.generated.ConnectorRolloutRead
import io.airbyte.api.client.model.generated.ConnectorRolloutState
import io.airbyte.api.client.model.generated.ConnectorRolloutStrategy
import io.airbyte.config.ConnectorEnumRolloutState
import io.airbyte.config.ConnectorEnumRolloutStrategy
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutOutput

object ConnectorRolloutActivityHelpers {
  fun mapToConnectorRollout(rolloutRead: ConnectorRolloutRead): ConnectorRolloutOutput {
    return ConnectorRolloutOutput(
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
      actorIds = ArrayList(),
    )
  }

  private fun mapState(state: ConnectorRolloutState): ConnectorEnumRolloutState {
    return state.let { ConnectorEnumRolloutState.valueOf(it.name) }
  }

  private fun mapRolloutStrategy(strategy: ConnectorRolloutStrategy?): ConnectorEnumRolloutStrategy? {
    return strategy?.let { ConnectorEnumRolloutStrategy.valueOf(it.name) }
  }
}
