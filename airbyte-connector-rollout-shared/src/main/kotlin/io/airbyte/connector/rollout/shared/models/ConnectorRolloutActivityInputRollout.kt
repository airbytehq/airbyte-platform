package io.airbyte.connector.rollout.shared.models

import java.util.UUID

data class ConnectorRolloutActivityInputRollout(
  var dockerRepository: String,
  var dockerImageTag: String,
  var actorDefinitionId: UUID,
  var rolloutId: UUID,
  var actorIds: List<UUID>,
  var updatedBy: UUID? = null,
)
