package io.airbyte.connector.rollout.worker.models

import java.util.UUID

data class ConnectorRolloutActivityInputRollout(
  var dockerRepository: String,
  var dockerImageTag: String,
  var actorDefinitionId: UUID,
  var rolloutId: UUID,
  var actorIds: List<UUID>,
)
