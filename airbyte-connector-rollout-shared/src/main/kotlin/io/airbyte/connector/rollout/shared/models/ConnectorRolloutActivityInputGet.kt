package io.airbyte.connector.rollout.shared.models

import java.util.UUID

data class ConnectorRolloutActivityInputGet(
  var dockerRepository: String,
  var dockerImageTag: String,
  var actorDefinitionId: UUID,
  var rolloutId: UUID,
)
