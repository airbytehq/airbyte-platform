package io.airbyte.data.repositories.entities

import io.airbyte.db.instance.configs.jooq.generated.enums.ConnectorRolloutStateType
import io.airbyte.db.instance.configs.jooq.generated.enums.ConnectorRolloutStrategyType
import io.micronaut.core.annotation.Nullable
import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import java.time.OffsetDateTime
import java.util.UUID

@MappedEntity("connector_rollout")
data class ConnectorRollout(
  @field:Id
  var id: UUID,
  var actorDefinitionId: UUID,
  var releaseCandidateVersionId: UUID,
  var initialVersionId: UUID? = null,
  var state: ConnectorRolloutStateType,
  var initialRolloutPct: Int,
  @Nullable
  var currentTargetRolloutPct: Int? = null,
  var finalTargetRolloutPct: Int,
  var hasBreakingChanges: Boolean,
  var rolloutStrategy: ConnectorRolloutStrategyType,
  var maxStepWaitTimeMins: Int,
  @Nullable
  var updatedBy: UUID? = null,
  @DateCreated
  var createdAt: OffsetDateTime? = null,
  @DateUpdated
  var updatedAt: OffsetDateTime? = null,
  var completedAt: OffsetDateTime? = null,
  var expiresAt: OffsetDateTime,
  @Nullable
  var errorMsg: String? = null,
  @Nullable
  var failedReason: String? = null,
)
