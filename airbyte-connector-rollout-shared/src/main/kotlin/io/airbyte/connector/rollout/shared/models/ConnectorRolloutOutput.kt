package io.airbyte.connector.rollout.shared.models

import io.airbyte.api.model.generated.ConnectorRolloutActorSelectionInfo
import io.airbyte.api.model.generated.ConnectorRolloutActorSyncInfo
import io.airbyte.config.ConnectorEnumRolloutState
import io.airbyte.config.ConnectorEnumRolloutStrategy
import java.time.OffsetDateTime
import java.util.UUID

data class ConnectorRolloutOutput(
  var id: UUID? = null,
  var workflowRunId: String? = null,
  var actorDefinitionId: UUID? = null,
  var releaseCandidateVersionId: UUID? = null,
  var initialVersionId: UUID? = null,
  var state: ConnectorEnumRolloutState,
  var initialRolloutPct: Int? = null,
  var currentTargetRolloutPct: Int? = null,
  var finalTargetRolloutPct: Int? = null,
  var hasBreakingChanges: Boolean? = null,
  var rolloutStrategy: ConnectorEnumRolloutStrategy? = null,
  var maxStepWaitTimeMins: Int? = null,
  var updatedBy: String? = null,
  var createdAt: OffsetDateTime? = null,
  var updatedAt: OffsetDateTime? = null,
  var completedAt: OffsetDateTime? = null,
  var expiresAt: OffsetDateTime? = null,
  var errorMsg: String? = null,
  var failedReason: String? = null,
  var pausedReason: String? = null,
  var actorSelectionInfo: ConnectorRolloutActorSelectionInfo? = null,
  var actorSyncs: Map<UUID, ConnectorRolloutActorSyncInfo>? = null,
)
