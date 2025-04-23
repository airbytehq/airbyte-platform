/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data.mappers

import io.airbyte.config.AttributeName
import io.airbyte.config.CustomerTier
import io.airbyte.config.Operator
import io.airbyte.data.repositories.entities.ConnectorRollout
import io.airbyte.data.repositories.entities.ConnectorRolloutFilters
import io.airbyte.data.repositories.entities.CustomerTierFilter
import io.airbyte.data.repositories.entities.JobBypassFilter
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset

typealias EntityConnectorRolloutStateType = io.airbyte.db.instance.configs.jooq.generated.enums.ConnectorRolloutStateType
typealias ModelConnectorRolloutStateType = io.airbyte.config.ConnectorEnumRolloutState
typealias EntityConnectorRolloutStrategyType = io.airbyte.db.instance.configs.jooq.generated.enums.ConnectorRolloutStrategyType
typealias ModelConnectorRolloutStrategyType = io.airbyte.config.ConnectorEnumRolloutStrategy
typealias EntityConnectorRolloutFilters = ConnectorRolloutFilters
typealias ModelConnectorRolloutFilters = io.airbyte.config.ConnectorRolloutFilters
typealias ModelCustomerTierFilterExpression = io.airbyte.config.CustomerTierFilter
typealias ModelJobBypassFilter = io.airbyte.config.JobBypassFilter
typealias EntityJobBypassFilter = JobBypassFilter
typealias EntityConnectorRollout = ConnectorRollout
typealias ModelConnectorRollout = io.airbyte.config.ConnectorRollout

fun EntityConnectorRolloutStateType.toConfigModel(): ModelConnectorRolloutStateType =
  when (this) {
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

fun ModelConnectorRolloutStateType.toEntity(): EntityConnectorRolloutStateType =
  when (this) {
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

fun EntityConnectorRolloutStrategyType.toConfigModel(): ModelConnectorRolloutStrategyType =
  when (this) {
    EntityConnectorRolloutStrategyType.automated -> ModelConnectorRolloutStrategyType.AUTOMATED
    EntityConnectorRolloutStrategyType.manual -> ModelConnectorRolloutStrategyType.MANUAL
    EntityConnectorRolloutStrategyType.overridden -> ModelConnectorRolloutStrategyType.OVERRIDDEN
  }

fun ModelConnectorRolloutStrategyType.toEntity(): EntityConnectorRolloutStrategyType =
  when (this) {
    ModelConnectorRolloutStrategyType.AUTOMATED -> EntityConnectorRolloutStrategyType.automated
    ModelConnectorRolloutStrategyType.MANUAL -> EntityConnectorRolloutStrategyType.manual
    ModelConnectorRolloutStrategyType.OVERRIDDEN -> EntityConnectorRolloutStrategyType.overridden
  }

fun EntityConnectorRolloutFilters.toConfigModel(): ModelConnectorRolloutFilters {
  val customerTierFilters =
    this.customerTierFilters.map { attr ->
      try {
        val name = AttributeName.valueOf(attr.name)
        val operator = Operator.valueOf(attr.operator)

        val tierList =
          try {
            attr.value
              .asSequence()
              .map { tierValue ->
                tierValue.trim().let { CustomerTier.valueOf(it) }
              }.toList()
          } catch (e: Exception) {
            throw RuntimeException("Failed to parse tier(s) from: ${attr.value}", e)
          }

        ModelCustomerTierFilterExpression(
          name = name,
          operator = operator,
          value = tierList,
        )
      } catch (e: Exception) {
        throw RuntimeException("Failed to parse customer attribute: attribute=$attr exception=${e.message}", e)
      }
    }

  val jobBypassFilter =
    if (this.jobBypassFilter != null) {
      ModelJobBypassFilter(
        name = AttributeName.valueOf(this.jobBypassFilter.name),
        value = this.jobBypassFilter.value,
      )
    } else {
      null
    }

  return ModelConnectorRolloutFilters(customerTierFilters = customerTierFilters, jobBypassFilter = jobBypassFilter)
}

fun ModelConnectorRolloutFilters.toEntity(): EntityConnectorRolloutFilters {
  val customerTierFilters =
    this.customerTierFilters.map { filter ->
      when (filter) {
        is ModelCustomerTierFilterExpression -> {
          val name = filter.name.name
          val operator = filter.operator.name
          val value: List<String> = filter.value.map { it.name }

          CustomerTierFilter(
            name = name,
            operator = operator,
            value = value,
          )
        }
        else -> throw RuntimeException("Failed to parse customer attribute filter: filter=$filter")
      }
    }

  val jobBypassFilter =
    if (this.jobBypassFilter != null) {
      JobBypassFilter(
        name = this.jobBypassFilter!!.name.name,
        value = this.jobBypassFilter!!.value,
      )
    } else {
      null
    }
  return EntityConnectorRolloutFilters(customerTierFilters = customerTierFilters, jobBypassFilter = jobBypassFilter)
}

fun EntityConnectorRollout.toConfigModel(): ModelConnectorRollout =
  ModelConnectorRollout(
    id = this.id,
    workflowRunId = this.workflowRunId,
    actorDefinitionId = this.actorDefinitionId,
    releaseCandidateVersionId = this.releaseCandidateVersionId,
    initialVersionId = this.initialVersionId,
    state = this.state.toConfigModel(),
    initialRolloutPct = this.initialRolloutPct,
    currentTargetRolloutPct = this.currentTargetRolloutPct,
    finalTargetRolloutPct = this.finalTargetRolloutPct,
    hasBreakingChanges = this.hasBreakingChanges,
    rolloutStrategy = this.rolloutStrategy?.toConfigModel(),
    maxStepWaitTimeMins = this.maxStepWaitTimeMins,
    updatedBy = this.updatedBy,
    createdAt = this.createdAt!!.toEpochSecond(),
    updatedAt = this.updatedAt!!.toEpochSecond(),
    completedAt = this.completedAt?.toEpochSecond(),
    expiresAt = this.expiresAt?.toEpochSecond(),
    errorMsg = this.errorMsg,
    failedReason = this.failedReason,
    pausedReason = this.pausedReason,
    filters = this.filters?.toConfigModel(),
    tag = this.tag,
  )

fun ModelConnectorRollout.toEntity(): EntityConnectorRollout =
  EntityConnectorRollout(
    id = this.id,
    workflowRunId = this.workflowRunId,
    actorDefinitionId = this.actorDefinitionId,
    releaseCandidateVersionId = this.releaseCandidateVersionId,
    initialVersionId = this.initialVersionId,
    state = this.state.toEntity(),
    initialRolloutPct = this.initialRolloutPct,
    currentTargetRolloutPct = this.currentTargetRolloutPct,
    finalTargetRolloutPct = this.finalTargetRolloutPct,
    hasBreakingChanges = this.hasBreakingChanges,
    rolloutStrategy = this.rolloutStrategy?.toEntity(),
    maxStepWaitTimeMins = this.maxStepWaitTimeMins,
    updatedBy = this.updatedBy,
    createdAt = this.createdAt?.let { OffsetDateTime.ofInstant(Instant.ofEpochSecond(it), ZoneOffset.UTC) },
    updatedAt = this.updatedAt?.let { OffsetDateTime.ofInstant(Instant.ofEpochSecond(it), ZoneOffset.UTC) },
    completedAt = this.completedAt?.let { OffsetDateTime.ofInstant(Instant.ofEpochSecond(it), ZoneOffset.UTC) },
    expiresAt = this.expiresAt?.let { OffsetDateTime.ofInstant(Instant.ofEpochSecond(it), ZoneOffset.UTC) },
    errorMsg = this.errorMsg,
    failedReason = this.failedReason,
    pausedReason = this.pausedReason,
    filters = this.filters?.toEntity(),
    tag = this.tag,
  )
