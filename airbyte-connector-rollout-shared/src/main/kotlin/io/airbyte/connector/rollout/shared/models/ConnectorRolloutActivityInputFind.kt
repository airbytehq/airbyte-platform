package io.airbyte.connector.rollout.shared.models

import java.util.UUID

data class ConnectorRolloutActivityInputFind(
  var dockerRepository: String,
  var dockerImageTag: String,
  var actorDefinitionId: UUID,
)
