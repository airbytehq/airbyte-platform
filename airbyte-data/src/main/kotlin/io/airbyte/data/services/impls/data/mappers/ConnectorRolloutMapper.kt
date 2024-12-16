package io.airbyte.data.services.impls.data.mappers

import io.airbyte.data.repositories.entities.ConnectorRollout
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset

typealias EntityConnectorRolloutStateType = io.airbyte.db.instance.configs.jooq.generated.enums.ConnectorRolloutStateType
typealias ModelConnectorRolloutStateType = io.airbyte.config.ConnectorEnumRolloutState
typealias EntityConnectorRolloutStrategyType = io.airbyte.db.instance.configs.jooq.generated.enums.ConnectorRolloutStrategyType
typealias ModelConnectorRolloutStrategyType = io.airbyte.config.ConnectorEnumRolloutStrategy
typealias EntityConnectorRollout = ConnectorRollout
typealias ModelConnectorRollout = io.airbyte.config.ConnectorRollout

fun EntityConnectorRolloutStateType.toConfigModel(): ModelConnectorRolloutStateType {
  return when (this) {
    EntityConnectorRolloutStateType.initialized -> ModelConnectorRolloutStateType.INITIALIZED
    EntityConnectorRolloutStateType.workflow_started -> ModelConnectorRolloutStateType.WORKFLOW_STARTED
    EntityConnectorRolloutStateType.in_progress -> ModelConnectorRolloutStateType.IN_PROGRESS
    EntityConnectorRolloutStateType.paused -> ModelConnectorRolloutStateType.PAUSED
    EntityConnectorRolloutStateType.finalizing -> ModelConnectorRolloutStateType.FINALIZING
    EntityConnectorRolloutStateType.succeeded -> ModelConnectorRolloutStateType.SUCCEEDED
    EntityConnectorRolloutStateType.errored -> ModelConnectorRolloutStateType.ERRORED
    EntityConnectorRolloutStateType.failed_rolled_back -> ModelConnectorRolloutStateType.FAILED_ROLLED_BACK
    EntityConnectorRolloutStateType.canceled -> ModelConnectorRolloutStateType.CANCELED
  }
}

fun ModelConnectorRolloutStateType.toEntity(): EntityConnectorRolloutStateType {
  return when (this) {
    ModelConnectorRolloutStateType.INITIALIZED -> EntityConnectorRolloutStateType.initialized
    ModelConnectorRolloutStateType.WORKFLOW_STARTED -> EntityConnectorRolloutStateType.workflow_started
    ModelConnectorRolloutStateType.IN_PROGRESS -> EntityConnectorRolloutStateType.in_progress
    ModelConnectorRolloutStateType.PAUSED -> EntityConnectorRolloutStateType.paused
    ModelConnectorRolloutStateType.FINALIZING -> EntityConnectorRolloutStateType.finalizing
    ModelConnectorRolloutStateType.SUCCEEDED -> EntityConnectorRolloutStateType.succeeded
    ModelConnectorRolloutStateType.ERRORED -> EntityConnectorRolloutStateType.errored
    ModelConnectorRolloutStateType.FAILED_ROLLED_BACK -> EntityConnectorRolloutStateType.failed_rolled_back
    ModelConnectorRolloutStateType.CANCELED -> EntityConnectorRolloutStateType.canceled
  }
}

fun EntityConnectorRolloutStrategyType.toConfigModel(): ModelConnectorRolloutStrategyType {
  return when (this) {
    EntityConnectorRolloutStrategyType.automated -> ModelConnectorRolloutStrategyType.AUTOMATED
    EntityConnectorRolloutStrategyType.manual -> ModelConnectorRolloutStrategyType.MANUAL
    EntityConnectorRolloutStrategyType.overridden -> ModelConnectorRolloutStrategyType.OVERRIDDEN
  }
}

fun ModelConnectorRolloutStrategyType.toEntity(): EntityConnectorRolloutStrategyType {
  return when (this) {
    ModelConnectorRolloutStrategyType.AUTOMATED -> EntityConnectorRolloutStrategyType.automated
    ModelConnectorRolloutStrategyType.MANUAL -> EntityConnectorRolloutStrategyType.manual
    ModelConnectorRolloutStrategyType.OVERRIDDEN -> EntityConnectorRolloutStrategyType.overridden
  }
}

fun EntityConnectorRollout.toConfigModel(): ModelConnectorRollout {
  return ModelConnectorRollout()
    .withId(this.id)
    .withWorkflowRunId(this.workflowRunId)
    .withActorDefinitionId(this.actorDefinitionId)
    .withReleaseCandidateVersionId(this.releaseCandidateVersionId)
    .withInitialVersionId(this.initialVersionId)
    .withState(this.state.toConfigModel())
    .withInitialRolloutPct(this.initialRolloutPct?.toLong())
    .withCurrentTargetRolloutPct(this.currentTargetRolloutPct?.toLong())
    .withFinalTargetRolloutPct(this.finalTargetRolloutPct?.toLong())
    .withHasBreakingChanges(this.hasBreakingChanges)
    .withRolloutStrategy(this.rolloutStrategy?.toConfigModel())
    .withMaxStepWaitTimeMins(this.maxStepWaitTimeMins?.toLong())
    .withUpdatedBy(this.updatedBy)
    .withCreatedAt(this.createdAt?.toEpochSecond())
    .withUpdatedAt(this.updatedAt?.toEpochSecond())
    .withCompletedAt(this.completedAt?.toEpochSecond())
    .withExpiresAt(this.expiresAt?.toEpochSecond())
    .withErrorMsg(this.errorMsg)
    .withFailedReason(this.failedReason)
    .withPausedReason(this.pausedReason)
}

fun ModelConnectorRollout.toEntity(): EntityConnectorRollout {
  return EntityConnectorRollout(
    id = this.id,
    workflowRunId = this.workflowRunId,
    actorDefinitionId = this.actorDefinitionId,
    releaseCandidateVersionId = this.releaseCandidateVersionId,
    initialVersionId = this.initialVersionId,
    state = this.state.toEntity(),
    initialRolloutPct = this.initialRolloutPct?.toInt(),
    currentTargetRolloutPct = this.currentTargetRolloutPct?.toInt(),
    finalTargetRolloutPct = this.finalTargetRolloutPct?.toInt(),
    hasBreakingChanges = this.hasBreakingChanges,
    rolloutStrategy = this.rolloutStrategy?.toEntity(),
    maxStepWaitTimeMins = this.maxStepWaitTimeMins?.toInt(),
    updatedBy = this.updatedBy,
    createdAt = this.createdAt?.let { OffsetDateTime.ofInstant(Instant.ofEpochSecond(it), ZoneOffset.UTC) },
    updatedAt = this.updatedAt?.let { OffsetDateTime.ofInstant(Instant.ofEpochSecond(it), ZoneOffset.UTC) },
    completedAt = this.completedAt?.let { OffsetDateTime.ofInstant(Instant.ofEpochSecond(it), ZoneOffset.UTC) },
    expiresAt = this.expiresAt?.let { OffsetDateTime.ofInstant(Instant.ofEpochSecond(it), ZoneOffset.UTC) },
    errorMsg = this.errorMsg,
    failedReason = this.failedReason,
    pausedReason = this.pausedReason,
  )
}
