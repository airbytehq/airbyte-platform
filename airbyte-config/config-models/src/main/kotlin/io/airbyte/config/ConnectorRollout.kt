/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import java.util.UUID

data class ConnectorRollout(
  val id: UUID,
  var workflowRunId: String? = null,
  var actorDefinitionId: UUID,
  var releaseCandidateVersionId: UUID,
  var initialVersionId: UUID,
  var state: ConnectorEnumRolloutState,
  var initialRolloutPct: Int? = null,
  var currentTargetRolloutPct: Int? = null,
  var finalTargetRolloutPct: Int? = null,
  var hasBreakingChanges: Boolean,
  var rolloutStrategy: ConnectorEnumRolloutStrategy? = null,
  var maxStepWaitTimeMins: Int? = null,
  var updatedBy: UUID? = null,
  var createdAt: Long,
  var updatedAt: Long,
  var completedAt: Long? = null,
  var expiresAt: Long? = null,
  var errorMsg: String? = null,
  var failedReason: String? = null,
  var pausedReason: String? = null,
  var filters: ConnectorRolloutFilters? = null,
  var tag: String?,
)
