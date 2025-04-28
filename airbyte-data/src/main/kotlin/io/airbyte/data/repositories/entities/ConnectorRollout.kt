/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.entities

import io.airbyte.db.instance.configs.jooq.generated.enums.ConnectorRolloutStateType
import io.airbyte.db.instance.configs.jooq.generated.enums.ConnectorRolloutStrategyType
import io.micronaut.core.annotation.Nullable
import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import io.micronaut.data.annotation.TypeDef
import io.micronaut.data.model.DataType
import java.time.OffsetDateTime
import java.util.UUID

@MappedEntity("connector_rollout")
data class ConnectorRollout(
  @field:Id
  var id: UUID,
  var workflowRunId: String? = null,
  var actorDefinitionId: UUID,
  var releaseCandidateVersionId: UUID,
  var initialVersionId: UUID? = null,
  var state: ConnectorRolloutStateType,
  @Nullable
  var initialRolloutPct: Int? = 0,
  @Nullable
  var currentTargetRolloutPct: Int? = 0,
  @Nullable
  var finalTargetRolloutPct: Int? = 0,
  var hasBreakingChanges: Boolean,
  @Nullable
  var rolloutStrategy: ConnectorRolloutStrategyType? = null,
  @Nullable
  var maxStepWaitTimeMins: Int? = null,
  @Nullable
  var updatedBy: UUID? = null,
  @DateCreated
  var createdAt: OffsetDateTime? = null,
  @DateUpdated
  var updatedAt: OffsetDateTime? = null,
  @Nullable
  var completedAt: OffsetDateTime? = null,
  @Nullable
  var expiresAt: OffsetDateTime? = null,
  @Nullable
  var errorMsg: String? = null,
  @Nullable
  var failedReason: String? = null,
  @Nullable
  var pausedReason: String? = null,
  @Nullable
  @field:TypeDef(type = DataType.JSON)
  var filters: ConnectorRolloutFilters? = null,
  @Nullable
  var tag: String? = null,
)

data class ConnectorRolloutFilters(
  val customerTierFilters: List<CustomerTierFilter>,
  val jobBypassFilter: JobBypassFilter?,
)

data class CustomerTierFilter(
  val name: String,
  val operator: String,
  val value: List<String>,
)

data class JobBypassFilter(
  val name: String,
  val value: Boolean,
)
