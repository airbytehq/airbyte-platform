/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data.mappers

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.config.AttributeName
import io.airbyte.config.CustomerTier
import io.airbyte.config.Operator
import io.airbyte.data.repositories.entities.ConnectorRollout
import io.airbyte.data.repositories.entities.ConnectorRolloutFilters
import io.airbyte.data.repositories.entities.OrganizationCustomerAttributeFilter
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset

typealias EntityConnectorRolloutStateType = io.airbyte.db.instance.configs.jooq.generated.enums.ConnectorRolloutStateType
typealias ModelConnectorRolloutStateType = io.airbyte.config.ConnectorEnumRolloutState
typealias EntityConnectorRolloutStrategyType = io.airbyte.db.instance.configs.jooq.generated.enums.ConnectorRolloutStrategyType
typealias ModelConnectorRolloutStrategyType = io.airbyte.config.ConnectorEnumRolloutStrategy
typealias EntityConnectorRolloutFilters = ConnectorRolloutFilters
typealias ModelConnectorRolloutFilters = io.airbyte.config.ConnectorRolloutFilters
typealias ModelCustomerAttributeFilterExpression = io.airbyte.config.CustomerTierFilter
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
  val expressions =
    this.organizationCustomerAttributeFilters.map { attr ->
      try {
        val name = AttributeName.valueOf(attr.name.uppercase())
        val operator = Operator.valueOf(attr.operator.uppercase())

        val value =
          when (name) {
            AttributeName.TIER -> {
              val valuesNode =
                attr.value["value"]
                  ?: throw RuntimeException("Missing 'value' field in TIER attribute filter: ${attr.value}")

              val tierList =
                try {
                  valuesNode
                    .asSequence()
                    .mapNotNull { tierNode ->
                      tierNode?.asText()?.trim()?.let { CustomerTier.valueOf(it) }
                    }.toList()
                } catch (e: Exception) {
                  throw RuntimeException("Failed to parse tier(s) from: $valuesNode", e)
                }

              tierList
            }
          }

        ModelCustomerAttributeFilterExpression(
          name = name,
          operator = operator,
          value = value,
        )
      } catch (e: Exception) {
        throw RuntimeException("Failed to parse customer attribute: attribute=$attr exception=${e.message}", e)
      }
    }

  return ModelConnectorRolloutFilters(customerTierFilters = expressions)
}

fun ModelConnectorRolloutFilters.toEntity(): EntityConnectorRolloutFilters {
  val customerAttributeFilters =
    this.customerTierFilters.map { filter ->
      when (filter) {
        is ModelCustomerAttributeFilterExpression -> {
          val name = filter.name.toString()
          val operator = filter.operator.toString()
          val value: JsonNode = objectMapper.valueToTree(mapOf("value" to filter.value.map { it.name }))

          OrganizationCustomerAttributeFilter(
            name = name,
            operator = operator,
            value = value,
          )
        }
        else -> throw RuntimeException("Failed to parse customer attribute filter: filter=$filter")
      }
    }
  return EntityConnectorRolloutFilters(organizationCustomerAttributeFilters = customerAttributeFilters)
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
  )
