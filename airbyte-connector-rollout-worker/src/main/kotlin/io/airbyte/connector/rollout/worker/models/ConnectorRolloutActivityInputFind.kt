package io.airbyte.connector.rollout.worker.models

import java.util.UUID

data class ConnectorRolloutActivityInputFind(
  var dockerRepository: String,
  var dockerImageTag: String,
  var actorDefinitionId: UUID,
)
